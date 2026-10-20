//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/result"
)

func TestMatchResult_IdempotencyScopingAndHardening(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Boot PostgreSQL and Redis Testcontainers with all migrations applied (including 000009)
	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	resultStore := result.NewPostgresResultStore(harness.PGPool)
	handler := result.NewHandler(resultStore)

	r := chi.NewRouter()
	handler.RegisterRoutes(r)

	// 2. Seed a Match and 2 Players in Postgres
	matchID := uuid.New()
	player1ID := uuid.New()
	player2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'Player1', 'NA', 1200, 'IDLE'),
		       ($2, 'Player2', 'NA', 1200, 'IDLE')
	`, player1ID, player2ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO matches (id, region, status)
		VALUES ($1, 'NA', 'IN_PROGRESS')
	`, matchID)
	if err != nil {
		t.Fatalf("failed to seed match: %v", err)
	}

	sharedKey := "shared-client-key-999"

	// 3. STEP A: Player 1 submits result with sharedKey -> 201 Created
	p1Body, _ := json.Marshal(map[string]interface{}{
		"player_id": player1ID.String(),
		"score":     100,
		"result":    "WIN",
	})
	req1 := httptest.NewRequest("POST", "/api/v1/matches/"+matchID.String()+"/result", bytes.NewReader(p1Body))
	req1.Header.Set("Idempotency-Key", sharedKey)
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	r.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for player 1 fresh submission, got %d: %s", rec1.Code, rec1.Body.String())
	}

	// 4. STEP B: Player 1 submits exact same request with sharedKey again -> 200 OK (idempotent cached replay)
	req1Dup := httptest.NewRequest("POST", "/api/v1/matches/"+matchID.String()+"/result", bytes.NewReader(p1Body))
	req1Dup.Header.Set("Idempotency-Key", sharedKey)
	req1Dup.Header.Set("Content-Type", "application/json")
	rec1Dup := httptest.NewRecorder()
	r.ServeHTTP(rec1Dup, req1Dup)

	if rec1Dup.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for duplicate idempotent request, got %d: %s", rec1Dup.Code, rec1Dup.Body.String())
	}

	// 5. STEP C: Player 1 tries to submit a second result for the SAME match using a DIFFERENT key -> 409 Conflict
	// This proves that a different key cannot bypass the match_results UNIQUE(match_id, player_id) constraint!
	p1DifferentKeyBody, _ := json.Marshal(map[string]interface{}{
		"player_id": player1ID.String(),
		"score":     150,
		"result":    "WIN",
	})
	req1DiffKey := httptest.NewRequest("POST", "/api/v1/matches/"+matchID.String()+"/result", bytes.NewReader(p1DifferentKeyBody))
	req1DiffKey.Header.Set("Idempotency-Key", "completely-different-key-111")
	req1DiffKey.Header.Set("Content-Type", "application/json")
	rec1DiffKey := httptest.NewRecorder()
	r.ServeHTTP(rec1DiffKey, req1DiffKey)

	if rec1DiffKey.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when submitting second result with different key, got %d: %s", rec1DiffKey.Code, rec1DiffKey.Body.String())
	}

	// Verify only 1 result exists for Player 1 in match_results
	var p1ResultCount int
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM match_results WHERE match_id = $1 AND player_id = $2`, matchID, player1ID).Scan(&p1ResultCount)
	if err != nil {
		t.Fatalf("failed to query match_results count: %v", err)
	}
	if p1ResultCount != 1 {
		t.Fatalf("expected exactly 1 result for player 1, got %d", p1ResultCount)
	}

	// 6. STEP D: Player 2 submits for Match 1 using the EXACT SAME key string (sharedKey) -> 201 Created
	// This proves that key scoping prevents collisions between distinct players!
	p2Body, _ := json.Marshal(map[string]interface{}{
		"player_id": player2ID.String(),
		"score":     50,
		"result":    "LOSS",
	})
	req2 := httptest.NewRequest("POST", "/api/v1/matches/"+matchID.String()+"/result", bytes.NewReader(p2Body))
	req2.Header.Set("Idempotency-Key", sharedKey) // same key as player 1
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created for player 2 using same key string, got %d: %s", rec2.Code, rec2.Body.String())
	}

	// Verify both results exist in match_results
	var totalResults int
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM match_results WHERE match_id = $1`, matchID).Scan(&totalResults)
	if err != nil {
		t.Fatalf("failed to count total match_results: %v", err)
	}
	if totalResults != 2 {
		t.Fatalf("expected exactly 2 results in match_results, got %d", totalResults)
	}

	// Verify both idempotency records exist in Postgres with different scope_ids
	var idemKeyCount int
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM idempotency_keys WHERE idempotency_key = $1`, sharedKey).Scan(&idemKeyCount)
	if err != nil {
		t.Fatalf("failed to count idempotency_keys: %v", err)
	}
	if idemKeyCount != 2 {
		t.Fatalf("expected 2 distinct idempotency_keys records for sharedKey (one per player scope), got %d", idemKeyCount)
	}
}
