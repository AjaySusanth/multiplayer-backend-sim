package matchmaking

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestQueueExpiryWorker_Sweep(t *testing.T) {
	ctx := context.Background()
	fakeStore := NewFakeQueueStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker := NewQueueExpiryWorker(fakeStore, logger, 50*time.Millisecond)

	now := time.Now().UTC()

	// 1. Seed Entry A: Expired (TTL = -1 minute)
	entryA, err := fakeStore.Create(ctx, CreateQueueEntryInput{
		PlayerID:    "player-a",
		SkillRating: 1200,
		Region:      "NA",
		TTL:         -1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create entry A: %v", err)
	}

	// 2. Seed Entry B: Active / Fresh (TTL = 5 minutes)
	entryB, err := fakeStore.Create(ctx, CreateQueueEntryInput{
		PlayerID:    "player-b",
		SkillRating: 1250,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create entry B: %v", err)
	}

	// 3. Seed Entry C: Already Matched (even if past TTL, it must remain MATCHED)
	entryC, err := fakeStore.Create(ctx, CreateQueueEntryInput{
		PlayerID:    "player-c",
		SkillRating: 1300,
		Region:      "NA",
		TTL:         -1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create entry C: %v", err)
	}
	if err := fakeStore.UpdateStatus(ctx, entryC.ID, QueueStatusMatched); err != nil {
		t.Fatalf("failed to update entry C status: %v", err)
	}

	// 4. Run Sweep
	count, err := worker.Sweep(ctx, now)
	if err != nil {
		t.Fatalf("unexpected error during sweep: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 expired entry, got %d", count)
	}

	// 5. Verify state invariants
	latestA, _ := fakeStore.GetLatestByPlayerID(ctx, "player-a")
	if latestA.Status != QueueStatusExpired {
		t.Errorf("expected entry A to be EXPIRED, got %s", latestA.Status)
	}

	latestB, _ := fakeStore.GetLatestByPlayerID(ctx, "player-b")
	if latestB.Status != QueueStatusQueued {
		t.Errorf("expected entry B to remain QUEUED, got %s", latestB.Status)
	}

	latestC, _ := fakeStore.GetLatestByPlayerID(ctx, "player-c")
	if latestC.Status != QueueStatusMatched {
		t.Errorf("expected entry C to remain MATCHED, got %s", latestC.Status)
	}

	_ = entryA
	_ = entryB
	_ = entryC
}

func TestQueueExpiryWorker_GracefulShutdown(t *testing.T) {
	fakeStore := NewFakeQueueStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	worker := NewQueueExpiryWorker(fakeStore, logger, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()

	// Allow the loop to tick once or twice
	time.Sleep(30 * time.Millisecond)

	// Cancel context to initiate graceful shutdown
	cancel()

	select {
	case <-done:
		// Clean termination
	case <-time.After(500 * time.Millisecond):
		t.Fatal("worker did not terminate gracefully within 500ms")
	}
}
