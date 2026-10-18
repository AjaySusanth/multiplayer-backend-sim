//go:build integration
package integration


import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/result"
)

func TestMatchResultIdeompotency_Concurrent(t *testing.T) {
	ctx,cancel := context.WithTimeout(context.Background(),90*time.Second)
	defer cancel()

	harness,teardown,err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	store := result.NewPostgresResultStore(harness.PGPool)
	playerID := uuid.New()
	matchID := uuid.New()
	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'TestPlayer', 'NA', 1200, 'IN_GAME')
	`, playerID)
	if err != nil {
		t.Fatalf("failed to seed player: %v", err)
	}
	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO matches (id, region, status, created_at)
		VALUES ($1, 'NA', 'ACTIVE', CURRENT_TIMESTAMP)
	`, matchID)
	if err != nil {
		t.Fatalf("failed to seed match: %v", err)
	}

	idempotencyKey := "test-idemp-key-" + uuid.New().String()
	operation := fmt.Sprintf("POST:/api/v1/matches/%s/result", matchID)
	rawResponse := json.RawMessage(`{"status":"recorded","score":100}`)

	numGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	records := make([]*result.IdempotencyRecord,numGoroutines)
	errors := make([]error,numGoroutines)


	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			matchResult := &result.MatchResult{
				ID:             uuid.New(),
				MatchID:        matchID,
				PlayerID:       playerID,
				Score:          100,
				Result:         "WIN",
				SubmittedAt:    time.Now().UTC(),
				IdempotencyKey: &idempotencyKey,
			}
			// Call the actual method on PostgresResultStore:
			rec, err := store.SaveResultIdempotent(ctx, matchResult, operation, rawResponse)
			records[idx] = rec
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	freshInserts := 0
	cachedHits := 0
	for i := 0; i < numGoroutines; i++ {
		if errors[i] != nil {
			t.Fatalf("goroutine %d failed unexpectedly: %v", i, errors[i])
		}
		if records[i] == nil {
			freshInserts++
		} else {
			cachedHits++
			var gotMap, wantMap map[string]any

			if err:= json.Unmarshal(records[i].ResponseJSON,&gotMap); err != nil {
				t.Fatalf("failed to unmarshal cached response: %v", err)
			}

			if err := json.Unmarshal(rawResponse, &wantMap); err != nil {
				t.Fatalf("failed to unmarshal expected response: %v", err)
			}

			if gotMap["status"]!= wantMap["status"] || gotMap["score"]!= wantMap["score"] {
				t.Errorf("goroutine %d cached response mismatch: got %v, want %v", i, gotMap, wantMap)
				
			}
		}
	}

	if freshInserts != 1 {
		t.Errorf("expected exactly 1 fresh insert (rec == nil), got %d", freshInserts)
	}
	if cachedHits != numGoroutines-1 {
		t.Errorf("expected %d cached hits (rec != nil), got %d", numGoroutines-1, cachedHits)
	}
	// 5. Verify directly in real Postgres that only 1 row exists in match_results
	var rowCount int
	err = harness.PGPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM match_results WHERE match_id = $1 AND player_id = $2
	`, matchID, playerID).Scan(&rowCount)
	if err != nil {
		t.Fatalf("failed to query match_results count: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected exactly 1 row in match_results, found %d", rowCount)
	}
}