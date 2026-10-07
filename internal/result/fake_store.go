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
	idempotencyCache map[string]*IdempotencyRecord
}

func NewFakeResultStore() *FakeResultStore {
	return &FakeResultStore{
		results:          make(map[uuid.UUID]*MatchResult),
		idempotencyCache: make(map[string]*IdempotencyRecord),
	}
}

func (s *FakeResultStore) SaveResultIdempotent(ctx context.Context, res *MatchResult, operation string, responseJSON json.RawMessage) (*IdempotencyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if res.IdempotencyKey == nil {
		return nil, fmt.Errorf("idempotency key is required")
	}

	// Create a composite key for our in-memory map to mirror the (key, operation) 
	// primary key in Postgres.
	cacheKey := fmt.Sprintf("%s|%s", *res.IdempotencyKey, operation)

	// Simulate hitting the unique constraint and fetching the existing response
	if existing, found := s.idempotencyCache[cacheKey]; found {
		return existing, nil
	}

	// Simulate the successful transaction: saving both the idempotency response
	// and the match result simultaneously.
	s.idempotencyCache[cacheKey] = &IdempotencyRecord{
		IdempotencyKey: *res.IdempotencyKey,
		Operation:      operation,
		ResponseJSON:   responseJSON,
	}

	s.results[res.ID] = res

	return nil, nil
}