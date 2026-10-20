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

func TestMatchmaking_QueueWaitExpiryLoop(t *testing.T) {
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

	// 2. Seed 2 players in Postgres
	player1ID := uuid.New()
	player2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'ExpiringPlayer', 'NA', 1200, 'IDLE'),
		       ($2, 'ActivePlayer', 'NA', 1200, 'IDLE')
	`, player1ID, player2ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}

	// 3. Create Ticket 1 with a short 100ms TTL (will expire shortly)
	entry1, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
		TTL:         100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create queue entry for player 1: %v", err)
	}

	// 4. Create Ticket 2 with standard 5-minute TTL (must remain active)
	entry2, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player2ID.String(),
		SkillRating: 1200,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create queue entry for player 2: %v", err)
	}

	// 5. Start QueueExpiryWorker with a 50ms sweep interval
	worker := matchmaking.NewQueueExpiryWorker(queueStore, logger, 50*time.Millisecond)

	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.Run(workerCtx)
	}()

	// 6. Poll until Ticket 1 transitions to EXPIRED in PostgreSQL
	var expired bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		err := harness.PGPool.QueryRow(ctx, `SELECT status FROM queue_entries WHERE id = $1`, entry1.ID).Scan(&status)
		if err == nil && status == string(matchmaking.QueueStatusExpired) {
			expired = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !expired {
		t.Fatalf("expected queue entry %s to be transitioned to EXPIRED by background loop", entry1.ID)
	}

	// 7. Verify Ticket 2 is still QUEUED (not prematurely expired)
	var status2 string
	err = harness.PGPool.QueryRow(ctx, `SELECT status FROM queue_entries WHERE id = $1`, entry2.ID).Scan(&status2)
	if err != nil {
		t.Fatalf("failed to query status of ticket 2: %v", err)
	}
	if status2 != string(matchmaking.QueueStatusQueued) {
		t.Fatalf("expected ticket 2 to remain QUEUED, got %s", status2)
	}

	// 8. Verify player 1 can now re-queue because the partial index WHERE status='QUEUED' is freed
	newEntry, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("expected player 1 to be able to re-queue after ticket expired: %v", err)
	}
	if newEntry.Status != matchmaking.QueueStatusQueued {
		t.Fatalf("expected new entry to be QUEUED, got %s", newEntry.Status)
	}

	// 9. Verify graceful shutdown of QueueExpiryWorker
	cancelWorker()
	select {
	case <-workerDone:
		// worker loop returned cleanly
	case <-time.After(1 * time.Second):
		t.Fatal("expected queue expiry worker to terminate gracefully upon context cancellation")
	}
}
