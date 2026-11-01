//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"multiplayer-backend-sim/internal/matchmaking"
)

// TestMatchmaking_ExpiryMatchRace provokes the race condition where a queue ticket expires
// after a job was dispatched to Redis, and asserts that:
// 1. CreateMatch detects the expired ticket, aborts, and rolls back atomically (0 matches created).
// 2. The expired player's stream message is cleanly acknowledged out of Redis PEL.
// 3. The healthy player's ticket remains QUEUED with 0 retry attempt counter.
// 4. When a subsequent valid candidate joins, the healthy player is successfully matched.
func TestMatchmaking_ExpiryMatchRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Boot PostgreSQL and Redis Testcontainers
	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	publisher := matchmaking.NewRedisQueuePublisher(harness.RedisClient)

	// 2. Start Worker with fast staleTimeout and blockTimeout
	workerName := "race_worker"
	consumer := matchmaking.NewConsumer(
		harness.RedisClient, queueStore, matchStore, discardLogger, workerName,
	).WithStaleTimeout(50 * time.Millisecond).WithBlockTimeout(50 * time.Millisecond)

	if err := consumer.SetupGroup(ctx); err != nil {
		t.Fatalf("failed to setup consumer group: %v", err)
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	go consumer.Run(workerCtx)

	// 3. Seed Player 1 and Player 2 in PostgreSQL
	player1ID := uuid.New()
	player2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES 
			($1, 'RacePlayer1', 'NA', 1200, 'IDLE'),
			($2, 'RacePlayer2', 'NA', 1250, 'IDLE')
	`, player1ID, player2ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}

	entry1, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 1: %v", err)
	}

	entry2, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player2ID.String(),
		SkillRating: 1250,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 2: %v", err)
	}

	// 4. Simulate the race: Player 2's ticket is transitioned to EXPIRED
	// (as would happen if QueueExpiryWorker swept it before the match transaction committed)
	_, err = harness.PGPool.Exec(ctx, `
		UPDATE queue_entries SET status = 'EXPIRED' WHERE id = $1
	`, entry2.ID)
	if err != nil {
		t.Fatalf("failed to expire player 2 ticket: %v", err)
	}

	// 5. Publish both jobs to Redis Stream
	err = publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to publish player 1 job: %v", err)
	}

	err = publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player2ID.String(),
		SkillRating: 1250,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to publish player 2 job: %v", err)
	}

	// 6. Poll until Player 2's dead message is ACKed out of the PEL
	// Player 1's message must remain in the PEL (Count == 1)
	deadline := time.Now().Add(5 * time.Second)
	var raceHandled bool

	for time.Now().Before(deadline) {
		pending, pErr := harness.RedisClient.XPending(ctx, matchmaking.MatchmakingStreamKey, matchmaking.ConsumerGroupName).Result()
		if pErr == nil && pending.Count == 1 {
			// Exactly 1 message left in PEL (Player 1)
			pelEntries, _ := harness.RedisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: matchmaking.MatchmakingStreamKey,
				Group:  matchmaking.ConsumerGroupName,
				Start:  "-",
				End:    "+",
				Count:  5,
			}).Result()
			if len(pelEntries) == 1 {
				raceHandled = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !raceHandled {
		t.Fatalf("expected PEL to drop to 1 pending message after Player 2 expired ticket was ACKed")
	}

	// 7. Verify Invariants after race:

	// Invariant A: Exactly zero matches were created in PostgreSQL
	var matchCount int
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM matches`).Scan(&matchCount)
	if err != nil {
		t.Fatalf("failed to query matches count: %v", err)
	}
	if matchCount != 0 {
		t.Fatalf("expected 0 matches in database after race rollback, got %d", matchCount)
	}

	// Invariant B: Player 1 remains QUEUED
	p1Status, err := queueStore.GetActiveByPlayerID(ctx, player1ID.String())
	if err != nil {
		t.Fatalf("expected player 1 to remain active in queue: %v", err)
	}
	if p1Status.Status != matchmaking.QueueStatusQueued {
		t.Fatalf("expected player 1 status QUEUED, got %s", p1Status.Status)
	}

	// Invariant C: Player 2 remains EXPIRED
	var p2Status string
	err = harness.PGPool.QueryRow(ctx, `SELECT status FROM queue_entries WHERE id = $1`, entry2.ID).Scan(&p2Status)
	if err != nil {
		t.Fatalf("failed to query player 2 status: %v", err)
	}
	if p2Status != string(matchmaking.QueueStatusExpired) {
		t.Fatalf("expected player 2 status EXPIRED, got %s", p2Status)
	}

	// Invariant D: Zero application attempt counter keys exist (Player 1 was not penalized)
	attemptKeys, _ := harness.RedisClient.Keys(ctx, "matchmaking:attempts:*").Result()
	if len(attemptKeys) > 0 {
		t.Fatalf("expected 0 attempt keys, found: %v", attemptKeys)
	}

	// 8. Now introduce a valid Player 3 and prove Player 1 successfully matches
	player3ID := uuid.New()
	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'RacePlayer3', 'NA', 1210, 'IDLE')
	`, player3ID)
	if err != nil {
		t.Fatalf("failed to seed player 3: %v", err)
	}

	entry3, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player3ID.String(),
		SkillRating: 1210,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 3: %v", err)
	}

	err = publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player3ID.String(),
		SkillRating: 1210,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to publish player 3 job: %v", err)
	}

	// 9. Poll until Player 1 and Player 3 are matched together in PostgreSQL
	var matchedMatchID string
	var matchFormed bool
	matchDeadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(matchDeadline) {
		row := harness.PGPool.QueryRow(ctx, `
			SELECT match_id 
			FROM match_players 
			WHERE player_id IN ($1, $2)
			GROUP BY match_id 
			HAVING COUNT(DISTINCT player_id) = 2
		`, player1ID, player3ID)

		if err := row.Scan(&matchedMatchID); err == nil {
			matchFormed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !matchFormed {
		t.Fatalf("expected player 1 and player 3 to form a match after player 1 was recovered from PEL")
	}

	// 10. Verify both tickets in PostgreSQL are now MATCHED
	var s1, s3 string
	_ = harness.PGPool.QueryRow(ctx, `SELECT status FROM queue_entries WHERE id = $1`, entry1.ID).Scan(&s1)
	_ = harness.PGPool.QueryRow(ctx, `SELECT status FROM queue_entries WHERE id = $1`, entry3.ID).Scan(&s3)

	if s1 != string(matchmaking.QueueStatusMatched) || s3 != string(matchmaking.QueueStatusMatched) {
		t.Fatalf("expected both player 1 and player 3 tickets to be MATCHED, got s1=%s, s3=%s", s1, s3)
	}

	// 11. Verify Redis PEL is now completely clear (0 pending)
	finalPending, err := harness.RedisClient.XPending(ctx, matchmaking.MatchmakingStreamKey, matchmaking.ConsumerGroupName).Result()
	if err != nil {
		t.Fatalf("failed to check final PEL: %v", err)
	}
	if finalPending.Count != 0 {
		t.Fatalf("expected PEL to be completely cleared (0 pending), got %d", finalPending.Count)
	}
}
