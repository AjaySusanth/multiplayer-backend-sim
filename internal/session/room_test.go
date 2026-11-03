package session

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"multiplayer-backend-sim/internal/metrics"
	"multiplayer-backend-sim/internal/result"
)

func TestRoom_LobbyTimeoutAbortsSession(t *testing.T) {
	metrics.Init()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeStore := NewFakeSessionStore()
	fakeResultStore := result.NewFakeResultStore()

	sess, _ := fakeStore.Create(context.Background(), "match-timeout-1")

	manualTicker := NewManualTicker()
	roomClosed := make(chan struct{})

	room := NewRoom(sess.ID, sess.MatchID, []string{"p1", "p2"}, fakeStore, fakeResultStore, logger, func(id string) {
		close(roomClosed)
	}).WithTicker(manualTicker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go room.Run(ctx)

	// Single player connects, second player never joins
	client1 := &Client{PlayerID: "p1", Send: make(chan []byte, 10)}
	room.Register <- client1

	// Advance 149 ticks: should still be active
	for i := 0; i < LobbyTimeoutTicks-1; i++ {
		manualTicker.Tick()
	}

	time.Sleep(10 * time.Millisecond)
	updatedSess, _ := fakeStore.GetByID(context.Background(), sess.ID)
	if updatedSess.Status != SessionStatusStarting {
		t.Fatalf("expected status STARTING at tick 149, got %s", updatedSess.Status)
	}

	// 150th tick triggers lobby timeout (15s)
	manualTicker.Tick()

	select {
	case <-roomClosed:
		// Room shut down cleanly
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for room to shut down after lobby timeout")
	}

	// Verify database status is ABORTED
	finalSess, _ := fakeStore.GetByID(context.Background(), sess.ID)
	if finalSess.Status != SessionStatusAborted {
		t.Fatalf("expected session status ABORTED, got %s", finalSess.Status)
	}

	// Lobby timeout must NOT submit any results
	if fakeResultStore.Count() != 0 {
		t.Fatalf("expected 0 results on lobby timeout, got %d", fakeResultStore.Count())
	}
}

func TestRoom_TwoPlayersStartAndComplete(t *testing.T) {
	metrics.Init()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeStore := NewFakeSessionStore()
	fakeResultStore := result.NewFakeResultStore()

	sess, _ := fakeStore.Create(context.Background(), "match-full-1")

	manualTicker := NewManualTicker()
	roomClosed := make(chan struct{})

	room := NewRoom(sess.ID, sess.MatchID, []string{"p1", "p2"}, fakeStore, fakeResultStore, logger, func(id string) {
		close(roomClosed)
	}).WithTicker(manualTicker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go room.Run(ctx)

	client1 := &Client{PlayerID: "p1", Send: make(chan []byte, 500)}
	client2 := &Client{PlayerID: "p2", Send: make(chan []byte, 500)}

	room.Register <- client1
	room.Register <- client2

	// Wait for registration and IN_PROGRESS transition
	time.Sleep(20 * time.Millisecond)

	activeSess, _ := fakeStore.GetByID(context.Background(), sess.ID)
	if activeSess.Status != SessionStatusInProgress {
		t.Fatalf("expected session status IN_PROGRESS, got %s", activeSess.Status)
	}

	// Drive the full 300 ticks through manual ticker
	for i := 0; i < TotalRoundTicks; i++ {
		manualTicker.Tick()
	}

	select {
	case <-roomClosed:
		// Completed
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for room to finish after 300 ticks")
	}

	finishedSess, _ := fakeStore.GetByID(context.Background(), sess.ID)
	if finishedSess.Status != SessionStatusFinished {
		t.Fatalf("expected session status FINISHED, got %s", finishedSess.Status)
	}

	outcome := room.Game().Outcome
	if outcome == nil {
		t.Fatalf("expected game outcome to be set")
	}
	if outcome.WinnerID == "" || outcome.LoserID == "" {
		t.Fatalf("expected non-empty winner and loser: %+v", outcome)
	}

	// Verify server-side result submission
	if fakeResultStore.Count() != 2 {
		t.Fatalf("expected exactly 2 match results submitted, got %d", fakeResultStore.Count())
	}

	results := fakeResultStore.GetResults()
	hasWin, hasLoss := false, false
	for _, res := range results {
		if res.Result == "WIN" {
			hasWin = true
		}
		if res.Result == "LOSS" {
			hasLoss = true
		}
		if res.IdempotencyKey == nil || *res.IdempotencyKey == "" {
			t.Fatalf("expected non-empty idempotency key for result: %+v", res)
		}
	}
	if !hasWin || !hasLoss {
		t.Fatalf("expected 1 WIN and 1 LOSS result, got: %+v", results)
	}
}

func TestRoom_MidRoundDisconnectForfeit(t *testing.T) {
	metrics.Init()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeStore := NewFakeSessionStore()
	fakeResultStore := result.NewFakeResultStore()

	sess, _ := fakeStore.Create(context.Background(), "match-forfeit-1")

	manualTicker := NewManualTicker()
	roomClosed := make(chan struct{})

	room := NewRoom(sess.ID, sess.MatchID, []string{"p1", "p2"}, fakeStore, fakeResultStore, logger, func(id string) {
		close(roomClosed)
	}).WithTicker(manualTicker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go room.Run(ctx)

	client1 := &Client{PlayerID: "p1", Send: make(chan []byte, 100)}
	client2 := &Client{PlayerID: "p2", Send: make(chan []byte, 100)}

	room.Register <- client1
	room.Register <- client2

	time.Sleep(20 * time.Millisecond)

	// Step a few ticks
	for i := 0; i < 10; i++ {
		manualTicker.Tick()
	}

	// Client 1 disconnects mid-round -> triggers forfeit!
	room.Unregister <- client1

	select {
	case <-roomClosed:
		// Room shut down immediately
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for room to close after forfeit")
	}

	forfeitedSess, _ := fakeStore.GetByID(context.Background(), sess.ID)
	if forfeitedSess.Status != SessionStatusFinished {
		t.Fatalf("expected session status FINISHED on forfeit, got %s", forfeitedSess.Status)
	}

	outcome := room.Game().Outcome
	if outcome == nil {
		t.Fatalf("expected game outcome to be set on forfeit")
	}
	if outcome.WinnerID != "p2" || outcome.LoserID != "p1" {
		t.Fatalf("expected winner p2, loser p1, got %+v", outcome)
	}
	if outcome.Reason != "forfeit" {
		t.Fatalf("expected reason 'forfeit', got %s", outcome.Reason)
	}

	// Verify server-side forfeit results submitted
	if fakeResultStore.Count() != 2 {
		t.Fatalf("expected 2 forfeit results submitted, got %d", fakeResultStore.Count())
	}
}
