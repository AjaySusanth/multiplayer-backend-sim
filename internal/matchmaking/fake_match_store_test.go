package matchmaking

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FakeMatchStore is a thread-safe in-memory implementation of MatchStore for unit testing.
type FakeMatchStore struct {
	mu          sync.RWMutex
	matches     map[string]*Match
	ErrToReturn error
	nextID      int
}

// NewFakeMatchStore initializes a new FakeMatchStore instance.
func NewFakeMatchStore() *FakeMatchStore {
	return &FakeMatchStore{
		matches: make(map[string]*Match),
	}
}

// CreateMatch creates a match and assigns players in memory, simulating an atomic transaction.
func (f *FakeMatchStore) CreateMatch(ctx context.Context, region string, playerIDs []string) (*Match, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ErrToReturn != nil {
		return nil, f.ErrToReturn
	}

	f.nextID++
	id := fmt.Sprintf("match-%d", f.nextID)
	now := time.Now().UTC()

	m := &Match{
		ID:        id,
		Status:    MatchStatusCreated,
		Region:    region,
		Players:   make([]MatchPlayer, 0, len(playerIDs)),
		CreatedAt: now,
	}

	for _, pID := range playerIDs {
		m.Players = append(m.Players, MatchPlayer{
			PlayerID: pID,
			JoinedAt: now,
		})
	}

	f.matches[id] = m
	return m, nil
}

// GetByID retrieves a match from memory by ID.
func (f *FakeMatchStore) GetByID(ctx context.Context, matchID string) (*Match, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ErrToReturn != nil {
		return nil, f.ErrToReturn
	}

	m, ok := f.matches[matchID]
	if !ok {
		return nil, ErrMatchNotFound
	}

	return m, nil
}