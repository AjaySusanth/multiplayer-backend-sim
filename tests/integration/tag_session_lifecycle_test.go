//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"multiplayer-backend-sim/internal/matchmaking"
	"multiplayer-backend-sim/internal/metrics"
	"multiplayer-backend-sim/internal/result"
	"multiplayer-backend-sim/internal/session"
)

// TestTagSession_LifecycleAndStatusTransitions tests the database status transitions:
// 1. Session created in STARTING state.
// 2. Both players connect via WebSocket -> database session transitions to IN_PROGRESS.
// 3. Mid-round disconnect (forfeit) -> database session transitions to FINISHED.
func TestTagSession_LifecycleAndStatusTransitions(t *testing.T) {
	metrics.Init()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Seed 2 players in Postgres
	p1ID := uuid.New()
	p2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, skill_rating, region)
		VALUES 
			($1, 'tag-alice', 1500, 'eu-central'),
			($2, 'tag-bob', 1500, 'eu-central')
	`, p1ID, p2ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}

	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	_, _ = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    p1ID.String(),
		SkillRating: 1500,
		Region:      "eu-central",
	})
	_, _ = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    p2ID.String(),
		SkillRating: 1500,
		Region:      "eu-central",
	})

	// 2. Create match
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	match, err := matchStore.CreateMatch(ctx, "eu-central", []string{p1ID.String(), p2ID.String()})
	if err != nil {
		t.Fatalf("failed to create match: %v", err)
	}

	// 3. Create game session (starts in STARTING)
	sessionStore := session.NewPostgresSessionStore(harness.PGPool)
	gameSession, err := sessionStore.Create(ctx, match.ID)
	if err != nil {
		t.Fatalf("failed to create game session: %v", err)
	}

	if gameSession.Status != session.SessionStatusStarting {
		t.Fatalf("expected initial session status STARTING, got %s", gameSession.Status)
	}

	// 4. Mount Session Handler with real stores
	resultStore := result.NewPostgresResultStore(harness.PGPool)
	sessionHandler := session.NewHandler(sessionStore, matchStore, resultStore, discardLogger)
	r := chi.NewRouter()
	sessionHandler.RegisterRoutes(r)

	server := httptest.NewServer(r)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/session/" + gameSession.ID

	// 5. Connect Player 1
	conn1, _, err := websocket.DefaultDialer.Dial(wsURL+"?player_id="+p1ID.String(), nil)
	if err != nil {
		t.Fatalf("failed to connect player 1: %v", err)
	}
	defer conn1.Close()

	// Before Player 2 connects, session must still be STARTING in Postgres
	sessBeforeP2, err := sessionStore.GetByID(ctx, gameSession.ID)
	if err != nil {
		t.Fatalf("failed to get session: %v", err)
	}
	if sessBeforeP2.Status != session.SessionStatusStarting {
		t.Fatalf("expected status STARTING with only 1 player, got %s", sessBeforeP2.Status)
	}

	// 6. Connect Player 2
	conn2, _, err := websocket.DefaultDialer.Dial(wsURL+"?player_id="+p2ID.String(), nil)
	if err != nil {
		t.Fatalf("failed to connect player 2: %v", err)
	}
	defer conn2.Close()

	// Both connected: Poll until Postgres reflects IN_PROGRESS
	var sessInProgress *session.GameSession
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s, err := sessionStore.GetByID(ctx, gameSession.ID)
		if err == nil && s.Status == session.SessionStatusInProgress {
			sessInProgress = s
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if sessInProgress == nil {
		t.Fatalf("timed out waiting for session status to transition to IN_PROGRESS in database")
	}

	// 7. Mid-round disconnect: Player 1 closes connection (forfeit)
	conn1.Close()

	// Poll until Postgres reflects FINISHED
	var sessFinished *session.GameSession
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s, err := sessionStore.GetByID(ctx, gameSession.ID)
		if err == nil && s.Status == session.SessionStatusFinished {
			sessFinished = s
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if sessFinished == nil {
		t.Fatalf("timed out waiting for session status to transition to FINISHED on forfeit in database")
	}
	if sessFinished.FinishedAt == nil {
		t.Fatalf("expected finished_at timestamp to be set in database")
	}

	// 8. Verify authoritative match results were persisted directly by the room
	rows, err := harness.PGPool.Query(ctx, `
		SELECT player_id, result, idempotency_key
		FROM match_results
		WHERE match_id = $1
	`, match.ID)
	if err != nil {
		t.Fatalf("failed to query match_results: %v", err)
	}
	defer rows.Close()

	results := make(map[string]string)
	for rows.Next() {
		var pID, res, idemKey string
		if err := rows.Scan(&pID, &res, &idemKey); err != nil {
			t.Fatalf("failed to scan match_result: %v", err)
		}
		results[pID] = res
		expectedIdemKey := "match:" + match.ID + ":result:" + pID
		if idemKey != expectedIdemKey {
			t.Errorf("expected deterministic idempotency key %s, got %s", expectedIdemKey, idemKey)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected exactly 2 match results in database, got %d", len(results))
	}
	// Player 1 left -> LOSS; Player 2 stayed -> WIN
	if results[p1ID.String()] != "LOSS" {
		t.Errorf("expected leaver p1 to have LOSS, got %s", results[p1ID.String()])
	}
	if results[p2ID.String()] != "WIN" {
		t.Errorf("expected remaining p2 to have WIN, got %s", results[p2ID.String()])
	}
}
