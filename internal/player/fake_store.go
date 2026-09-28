package player

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FakePlayerStore is a thread-safe in-memory implementation of PlayerStore for fast unit testing.
type FakePlayerStore struct {
	mu          sync.RWMutex
	players     map[string]*Player
	ErrToReturn error
	nextID      int
}

// NewFakePlayerStore creates a new FakePlayerStore instance.
func NewFakePlayerStore() *FakePlayerStore {
	return &FakePlayerStore{
		players: make(map[string]*Player),
	}
}

// Create inserts a player into the in-memory map or returns ErrToReturn if configured.
func (f *FakePlayerStore) Create(ctx context.Context, input CreatePlayerInput) (*Player, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ErrToReturn != nil {
		return nil, f.ErrToReturn
	}

	for _, p := range f.players {
		if p.Name == input.Name {
			return nil, ErrDuplicatePlayerName
		}
	}

	f.nextID++
	id := fmt.Sprintf("player-%d", f.nextID)

	now := time.Now().UTC()
	p := &Player{
		ID:          id,
		Name:        input.Name,
		SkillRating: input.SkillRating,
		Region:      input.Region,
		Status:      StatusIdle,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	f.players[id] = p
	return p, nil
}

// GetByID retrieves a player from memory by ID or returns ErrPlayerNotFound.
func (f *FakePlayerStore) GetByID(ctx context.Context, id string) (*Player, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ErrToReturn != nil {
		return nil, f.ErrToReturn
	}

	p, ok := f.players[id]
	if !ok {
		return nil, ErrPlayerNotFound
	}

	return p, nil
}

// Delete removes a player from memory by ID or returns ErrPlayerNotFound.
func (f *FakePlayerStore) Delete(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ErrToReturn != nil {
		return f.ErrToReturn
	}

	if _, ok := f.players[id]; !ok {
		return ErrPlayerNotFound
	}

	delete(f.players, id)
	return nil
}
