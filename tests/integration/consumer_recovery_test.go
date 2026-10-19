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

func TestConsumerCrashAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// 1. Setup Postgres & Redis containers
	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	// 2. Seed 2 compatible players
	player1ID := uuid.New()
	player2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES 
			($1, 'Survivor1', 'NA', 1200, 'IDLE'),
			($2, 'Survivor2', 'NA', 1250, 'IDLE')
	`, player1ID, player2ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}

	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	publisher := matchmaking.NewRedisQueuePublisher(harness.RedisClient)
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Setup consumer group
	setupConsumer := matchmaking.NewConsumer(harness.RedisClient, queueStore, matchStore, discardLogger, "setup_worker")
	if err := setupConsumer.SetupGroup(ctx); err != nil {
		t.Fatalf("failed to setup consumer group: %v", err)
	}

	// 3. Enqueue both players in Postgres
	_, err = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 1: %v", err)
	}

	_, err = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player2ID.String(),
		SkillRating: 1250,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 2: %v", err)
	}

	// 4. Publish both jobs into the Redis Stream
	if err := publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
	}); err != nil {
		t.Fatalf("failed to publish player 1: %v", err)
	}

	if err := publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player2ID.String(),
		SkillRating: 1250,
		Region:      "NA",
	}); err != nil {
		t.Fatalf("failed to publish player 2: %v", err)
	}

	// 5. Simulate Worker 1 crashing:
	// Worker 1 reads the messages into its PEL via XReadGroup, but DOES NOT ACK THEM.
	crashedWorkerName := "crashed_worker"
	readStreams, err := harness.RedisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    matchmaking.ConsumerGroupName,
		Consumer: crashedWorkerName,
		Streams:  []string{matchmaking.MatchmakingStreamKey, ">"},
		Count:    10,
	}).Result()
	if err != nil {
		t.Fatalf("crashed worker failed to read stream: %v", err)
	}
	if len(readStreams) == 0 || len(readStreams[0].Messages) != 2 {
		t.Fatalf("expected crashed worker to read 2 messages, got %v", readStreams)
	}

	// 6. Verify that the messages are now pending in crashed_worker's PEL
	pendingInfo, err := harness.RedisClient.XPending(ctx, matchmaking.MatchmakingStreamKey, matchmaking.ConsumerGroupName).Result()
	if err != nil {
		t.Fatalf("failed to check pending messages: %v", err)
	}
	if pendingInfo.Count != 2 {
		t.Fatalf("expected 2 pending messages in PEL, got %d", pendingInfo.Count)
	}

	// 7. Wait briefly so the messages exceed a minimal idle duration for reclaiming
	time.Sleep(100 * time.Millisecond)

	// 8. Start Worker 2 (Recovery Worker):
	// Worker 2's Run loop automatically calls XAutoClaim, reclaims the stale messages
	// from crashed_worker's PEL, processes the batch, creates the match in Postgres, and ACKs them.
	recoveryWorkerName := "recovery_worker"
	consumer2Ctx, cancelConsumer2 := context.WithCancel(ctx)
	defer cancelConsumer2()

	consumer2 := matchmaking.NewConsumer(
		harness.RedisClient, queueStore, matchStore, discardLogger, recoveryWorkerName,
	).WithStaleTimeout(50 * time.Millisecond)

	go consumer2.Run(consumer2Ctx)

	// 9. Poll Postgres until the match is created by the recovery worker
	var matchFound bool
	var recoveredMatchID string
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		row := harness.PGPool.QueryRow(ctx, `
			SELECT match_id 
			FROM match_players 
			WHERE player_id IN ($1, $2)
			GROUP BY match_id 
			HAVING COUNT(DISTINCT player_id) = 2
		`, player1ID, player2ID)

		if err := row.Scan(&recoveredMatchID); err == nil {
			matchFound = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !matchFound {
		t.Fatalf("recovery worker failed to create match in postgres")
	}

	// 10. Wait for recovery worker to finish ACKing and verify PEL is now empty (0 pending)
	var pelClean bool
	pelDeadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(pelDeadline) {
		pInfo, err := harness.RedisClient.XPending(ctx, matchmaking.MatchmakingStreamKey, matchmaking.ConsumerGroupName).Result()
		if err == nil && pInfo.Count == 0 {
			pelClean = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !pelClean {
		t.Errorf("expected PEL to be completely cleared (0 pending), but messages remain un-ACKed")
	}
}