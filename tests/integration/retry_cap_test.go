//go:build integration

package integration

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/matchmaking"
)

func TestMatchmaking_PoisonMessageRetryCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Boot PostgreSQL and Redis Testcontainers
	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	publisher := matchmaking.NewRedisQueuePublisher(harness.RedisClient)

	// 2. Start Worker with fast staleTimeout (50ms) and blockTimeout (50ms)
	workerName := "retry_test_worker"
	consumer := matchmaking.NewConsumer(
		harness.RedisClient, queueStore, matchStore, logger, workerName,
	).WithStaleTimeout(50 * time.Millisecond).WithBlockTimeout(50 * time.Millisecond)

	if err := consumer.SetupGroup(ctx); err != nil {
		t.Fatalf("failed to setup consumer group: %v", err)
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	go consumer.Run(workerCtx)

	// 3. Seed Player 1 in Postgres, but intentionally do NOT seed Player 2.
	// When the worker pairs them, CreateMatch in Postgres will fail the foreign key
	// constraint on match_players.player_id every single attempt.
	player1ID := uuid.New()
	player2ID := uuid.New() // Intentionally omitted from the players table

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'ValidPlayer', 'NA', 1200, 'IDLE')
	`, player1ID)
	if err != nil {
		t.Fatalf("failed to seed player 1: %v", err)
	}

	_, err = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to create queue entry for player 1: %v", err)
	}

	// 4. Publish both jobs into the Redis Stream
	err = publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to publish player 1: %v", err)
	}

	err = publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player2ID.String(),
		SkillRating: 1200,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to publish player 2: %v", err)
	}

	// 5. Poll until all messages in the PEL are acknowledged (pending count == 0)
	// Because CreateMatch fails repeatedly, the worker will retry 3 times,
	// and on reaching MaxRetryAttempts (3), acknowledge the messages from the PEL.
	var pelCleared bool
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := harness.RedisClient.XPending(ctx, matchmaking.MatchmakingStreamKey, matchmaking.ConsumerGroupName).Result()
		if err == nil && pending.Count == 0 {
			// Verify that processing actually occurred and the attempt counter was touched
			keys, _ := harness.RedisClient.Keys(ctx, "matchmaking:attempts:*").Result()
			if len(keys) > 0 {
				pelCleared = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !pelCleared {
		t.Fatalf("expected PEL to be cleared after %d failed attempts, but pending messages remain", matchmaking.MaxRetryAttempts)
	}

	// 6. VERIFY INVARIANTS per ARCHITECTURE.md §3:

	// Invariant A: Stream entries are PRESERVED in the stream (XAck removes from PEL, never XDel)
	streamEntries, err := harness.RedisClient.XRange(ctx, matchmaking.MatchmakingStreamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("failed to read stream entries: %v", err)
	}
	if len(streamEntries) < 2 {
		t.Fatalf("expected at least 2 stream entries preserved for audit history, got %d", len(streamEntries))
	}

	// Invariant B: Attempt counter reached MaxRetryAttempts (3)
	for _, entry := range streamEntries {
		attemptVal, err := harness.RedisClient.Get(ctx, matchmaking.AttemptKey(entry.ID)).Int()
		if err != nil {
			t.Fatalf("expected attempt counter for msg %s: %v", entry.ID, err)
		}
		if attemptVal != matchmaking.MaxRetryAttempts {
			t.Fatalf("expected attempts to equal %d for msg %s, got %d", matchmaking.MaxRetryAttempts, entry.ID, attemptVal)
		}
	}

	// Invariant C: Transaction rolled back atomically — 0 matches created in PostgreSQL
	var matchCount int
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM matches`).Scan(&matchCount)
	if err != nil {
		t.Fatalf("failed to count matches: %v", err)
	}
	if matchCount != 0 {
		t.Fatalf("expected 0 matches in database, got %d", matchCount)
	}

	// Invariant D: Active queue entry for Player 1 was marked CANCELLED
	updatedEntry, err := queueStore.GetLatestByPlayerID(ctx, player1ID.String())
	if err != nil {
		t.Fatalf("failed to get latest queue entry: %v", err)
	}
	if updatedEntry.Status != matchmaking.QueueStatusCancelled {
		t.Fatalf("expected queue entry status to be CANCELLED, got %s", updatedEntry.Status)
	}
}
