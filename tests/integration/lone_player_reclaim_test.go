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

// TestMatchmaking_LonePlayerReclaim verifies that a lone player waiting in the matchmaking
// stream PEL across repeated XAUTOCLAIM reclaim cycles:
// 1. Never increments the application retry attempt counter (matchmaking:attempts:<msgID>).
// 2. Is never mistakenly cancelled as a poison message.
// 3. Remains in QUEUED status in PostgreSQL and pending in Redis PEL.
func TestMatchmaking_LonePlayerReclaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Boot PostgreSQL and Redis Testcontainers with migrations applied
	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	publisher := matchmaking.NewRedisQueuePublisher(harness.RedisClient)

	// 2. Start Worker with fast staleTimeout (50ms) and blockTimeout (50ms)
	// This forces XAutoClaim to cycle rapidly and reclaim stale pending jobs repeatedly.
	workerName := "lone_player_worker"
	consumer := matchmaking.NewConsumer(
		harness.RedisClient, queueStore, matchStore, discardLogger, workerName,
	).WithStaleTimeout(50 * time.Millisecond).WithBlockTimeout(50 * time.Millisecond)

	if err := consumer.SetupGroup(ctx); err != nil {
		t.Fatalf("failed to setup consumer group: %v", err)
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	go consumer.Run(workerCtx)

	// 3. Seed exactly 1 player in PostgreSQL with a standard 5-minute wait TTL
	playerID := uuid.New()
	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'SoloGamer', 'NA', 1200, 'IDLE')
	`, playerID)
	if err != nil {
		t.Fatalf("failed to seed solo player: %v", err)
	}

	queueEntry, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    playerID.String(),
		SkillRating: 1200,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create queue entry: %v", err)
	}

	// 4. Publish lone job to Redis Stream
	err = publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    playerID.String(),
		SkillRating: 1200,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to publish lone job: %v", err)
	}

	// 5. Poll until Redis internal delivery count in PEL indicates multiple reclaim passes have occurred.
	// With staleTimeout=50ms, 400-600ms is sufficient for 4+ delivery/reclaim attempts.
	deadline := time.Now().Add(5 * time.Second)
	var multipleReclaimsOccurred bool
	var observedDeliveries int64

	for time.Now().Before(deadline) {
		pelEntries, err := harness.RedisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: matchmaking.MatchmakingStreamKey,
			Group:  matchmaking.ConsumerGroupName,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()

		if err == nil && len(pelEntries) > 0 {
			observedDeliveries = pelEntries[0].RetryCount
			// RetryCount in XPendingExt reflects the number of times Redis has delivered
			// the message to consumers (initial delivery + subsequent XAutoClaim deliveries).
			if observedDeliveries >= 4 {
				multipleReclaimsOccurred = true
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !multipleReclaimsOccurred {
		t.Fatalf("expected at least 4 delivery cycles via XAutoClaim, but observed only %d", observedDeliveries)
	}

	// 6. Invariant Verification:

	// Invariant A: In PostgreSQL, ticket status must remain strictly QUEUED (never CANCELLED or EXPIRED)
	activeEntry, err := queueStore.GetActiveByPlayerID(ctx, playerID.String())
	if err != nil {
		t.Fatalf("expected queue entry to remain active in PostgreSQL: %v", err)
	}
	if activeEntry.ID != queueEntry.ID {
		t.Fatalf("queue entry ID mismatch: got %s, want %s", activeEntry.ID, queueEntry.ID)
	}
	if activeEntry.Status != matchmaking.QueueStatusQueued {
		t.Fatalf("expected queue entry status to remain QUEUED, got %s", activeEntry.Status)
	}

	// Invariant B: Application retry attempt key must NOT exist in Redis (no processing error occurred)
	attemptKeys, err := harness.RedisClient.Keys(ctx, "matchmaking:attempts:*").Result()
	if err != nil {
		t.Fatalf("failed to check attempt keys: %v", err)
	}
	if len(attemptKeys) > 0 {
		t.Fatalf("expected zero attempt keys for healthy unmatched job, found: %v", attemptKeys)
	}

	// Invariant C: Exactly zero matches were created in PostgreSQL
	var matchCount int
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM matches`).Scan(&matchCount)
	if err != nil {
		t.Fatalf("failed to count matches: %v", err)
	}
	if matchCount != 0 {
		t.Fatalf("expected 0 matches created, found %d", matchCount)
	}

	// Invariant D: Job must remain pending in Redis PEL (never prematurely ACKed)
	pendingInfo, err := harness.RedisClient.XPending(ctx, matchmaking.MatchmakingStreamKey, matchmaking.ConsumerGroupName).Result()
	if err != nil {
		t.Fatalf("failed to query stream PEL: %v", err)
	}
	if pendingInfo.Count != 1 {
		t.Fatalf("expected exactly 1 message in PEL, got %d", pendingInfo.Count)
	}
}
