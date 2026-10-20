package result

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// FakeResultStore provides a thread-safe, in-memory implementation of ResultStore
// intended strictly for table-driven unit tests.
type FakeResultStore struct {
	mu               sync.Mutex
	results          map[uuid.UUID]*MatchResult
	matchPlayerMap   map[string]bool // tracks "matchID:playerID" for uniqueness
	idempotencyCache map[string]*IdempotencyRecord
}

func NewFakeResultStore() *FakeResultStore {
	return &FakeResultStore{
		results:          make(map[uuid.UUID]*MatchResult),
		matchPlayerMap:   make(map[string]bool),
		idempotencyCache: make(map[string]*IdempotencyRecord),
	}
}

func (s *FakeResultStore) SaveResultIdempotent(ctx context.Context, res *MatchResult, operation string, responseJSON json.RawMessage) (*IdempotencyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if res.IdempotencyKey == nil {
		return nil, fmt.Errorf("idempotency key is required")
	}

	scopeID := res.PlayerID.String()

	// Create a composite key to mirror the (key, operation, scope_id) primary key in Postgres.
	cacheKey := fmt.Sprintf("%s|%s|%s", *res.IdempotencyKey, operation, scopeID)

	// Simulate hitting the idempotency unique constraint and returning the cached response
	if existing, found := s.idempotencyCache[cacheKey]; found {
		return existing, nil
	}

	// Simulate the uq_match_results_match_player constraint:
	// A different key cannot submit a second result for the same match and player.
	matchPlayerKey := fmt.Sprintf("%s:%s", res.MatchID.String(), scopeID)
	if s.matchPlayerMap[matchPlayerKey] {
		return nil, ErrResultAlreadySubmitted
	}

	// Fresh submission
	s.matchPlayerMap[matchPlayerKey] = true
	s.idempotencyCache[cacheKey] = &IdempotencyRecord{
		IdempotencyKey: *res.IdempotencyKey,
		Operation:      operation,
		ScopeID:        scopeID,
		ResponseJSON:   responseJSON,
	}

	s.results[res.ID] = res

	return nil, nil
}
