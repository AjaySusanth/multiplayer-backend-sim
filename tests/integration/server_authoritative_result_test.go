//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/matchmaking"
	"multiplayer-backend-sim/internal/metrics"
	"multiplayer-backend-sim/internal/result"
	"multiplayer-backend-sim/internal/session"
)

// TestServerAuthoritativeResults verifies:
// 1. The session room automatically submits authoritative WIN/LOSS outcomes directly to PostgreSQL match_results upon round completion.
// 2. Both submissions use deterministic idempotency keys: "match:<matchID>:result:<playerID>".
// 3. The client-facing HTTP endpoint POST /api/v1/matches/{id}/result is protected and rejects unauthorized requests with 403 Forbidden.
// 4. Authorized internal requests passing X-Internal-Secret are permitted.
func TestServerAuthoritativeResults(t *testing.T) {
	metrics.Init()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Seed 2 players in database
	p1ID := uuid.New()
	p2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, skill_rating, region)
		VALUES 
			($1, 'alice-authoritative', 1500, 'eu-central'),
			($2, 'bob-authoritative', 1500, 'eu-central')
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

	// 3. Create session & stores
	sessionStore := session.NewPostgresSessionStore(harness.PGPool)
	resultStore := result.NewPostgresResultStore(harness.PGPool)

	gameSession, err := sessionStore.Create(ctx, match.ID)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// 4. Run a simulated match room directly using ManualTicker
	manualTicker := session.NewManualTicker()
	roomClosed := make(chan struct{})

	room := session.NewRoom(
		gameSession.ID,
		match.ID,
		[]string{p1ID.String(), p2ID.String()},
		sessionStore,
		resultStore,
		discardLogger,
		func(id string) {
			close(roomClosed)
		},
	).WithTicker(manualTicker)

	roomCtx, cancelRoom := context.WithCancel(ctx)
	defer cancelRoom()

	go room.Run(roomCtx)

	// Connect both simulated clients
	c1 := &session.Client{PlayerID: p1ID.String(), Send: make(chan []byte, 500)}
	c2 := &session.Client{PlayerID: p2ID.String(), Send: make(chan []byte, 500)}

	room.Register <- c1
	room.Register <- c2

	time.Sleep(20 * time.Millisecond)

	// Step through all 300 ticks to complete the match
	for i := 0; i < session.TotalRoundTicks; i++ {
		manualTicker.Tick()
	}

	select {
	case <-roomClosed:
		// Room completed and closed
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for room to finish")
	}

	// 5. Verify authoritative match_results rows in PostgreSQL
	rows, err := harness.PGPool.Query(ctx, `
		SELECT player_id, result, score, idempotency_key
		FROM match_results
		WHERE match_id = $1
	`, match.ID)
	if err != nil {
		t.Fatalf("failed to query match_results: %v", err)
	}
	defer rows.Close()

	type playerOutcome struct {
		result string
		score  int
		key    string
	}
	results := make(map[string]playerOutcome)

	for rows.Next() {
		var pID, res, key string
		var score int
		if err := rows.Scan(&pID, &res, &score, &key); err != nil {
			t.Fatalf("failed to scan match result: %v", err)
		}
		results[pID] = playerOutcome{result: res, score: score, key: key}
	}

	if len(results) != 2 {
		t.Fatalf("expected exactly 2 match results in database, got %d", len(results))
	}

	// Check deterministic idempotency keys
	for pID, out := range results {
		expectedKey := fmt.Sprintf("match:%s:result:%s", match.ID, pID)
		if out.key != expectedKey {
			t.Errorf("expected key %s, got %s", expectedKey, out.key)
		}
	}

	// Exactly 1 winner and 1 loser
	hasWin, hasLoss := false, false
	for _, out := range results {
		if out.result == "WIN" {
			hasWin = true
		}
		if out.result == "LOSS" {
			hasLoss = true
		}
	}
	if !hasWin || !hasLoss {
		t.Fatalf("expected exactly 1 WIN and 1 LOSS, got %+v", results)
	}

	// 6. Test Endpoint Protection: POST /api/v1/matches/{id}/result
	const internalSecret = "top-secret-backend-key"
	resultHandler := result.NewHandler(resultStore, internalSecret)
	router := chi.NewRouter()
	resultHandler.RegisterRoutes(router)

	bodyBytes, _ := json.Marshal(map[string]interface{}{
		"player_id": p1ID.String(),
		"score":     9999,
		"result":    "WIN",
	})

	// Case A: Client calls without secret -> 403 Forbidden
	reqUnauth := httptest.NewRequest("POST", "/api/v1/matches/"+match.ID+"/result", bytes.NewReader(bodyBytes))
	reqUnauth.Header.Set("Idempotency-Key", "client-spoof-attempt")
	reqUnauth.Header.Set("Content-Type", "application/json")
	recUnauth := httptest.NewRecorder()
	router.ServeHTTP(recUnauth, reqUnauth)

	if recUnauth.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for unauthorized client result submission, got %d", recUnauth.Code)
	}

	// Case B: Client calls with invalid secret -> 403 Forbidden
	reqWrong := httptest.NewRequest("POST", "/api/v1/matches/"+match.ID+"/result", bytes.NewReader(bodyBytes))
	reqWrong.Header.Set("X-Internal-Secret", "wrong-secret")
	reqWrong.Header.Set("Idempotency-Key", "client-spoof-attempt-2")
	reqWrong.Header.Set("Content-Type", "application/json")
	recWrong := httptest.NewRecorder()
	router.ServeHTTP(recWrong, reqWrong)

	if recWrong.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for wrong internal secret, got %d", recWrong.Code)
	}

	// Case C: Authorized internal service calls with valid X-Internal-Secret
	// Since results were already submitted by the room, duplicate submission returns 409 Conflict (or 200 if same key)
	reqAuth := httptest.NewRequest("POST", "/api/v1/matches/"+match.ID+"/result", bytes.NewReader(bodyBytes))
	reqAuth.Header.Set("X-Internal-Secret", internalSecret)
	reqAuth.Header.Set("Idempotency-Key", "new-internal-key")
	reqAuth.Header.Set("Content-Type", "application/json")
	recAuth := httptest.NewRecorder()
	router.ServeHTTP(recAuth, reqAuth)

	// Returns 409 Conflict because UNIQUE(match_id, player_id) prevents re-submitting a result for p1!
	if recAuth.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict because room already submitted result for player 1, got %d: %s", recAuth.Code, recAuth.Body.String())
	}
}
