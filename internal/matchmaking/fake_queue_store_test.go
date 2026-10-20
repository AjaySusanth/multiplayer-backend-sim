package matchmaking

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// FakeQueueStore is a thread-safe in-memory implementation of QueueStore for unit testing.
type FakeQueueStore struct {
	mu          sync.RWMutex
	entries     map[string]*QueueEntry
	ErrToReturn error
	nextID      int
}

// NewFakeQueueStore initializes a new FakeQueueStore instance.
func NewFakeQueueStore() *FakeQueueStore {
	return &FakeQueueStore{
		entries: make(map[string]*QueueEntry),
	}
}

// Create inserts a queue entry into memory, simulating the active-queue uniqueness constraint.
func (f *FakeQueueStore) Create(ctx context.Context, input CreateQueueEntryInput) (*QueueEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ErrToReturn != nil {
		return nil, f.ErrToReturn
	}

	// Simulate the PostgreSQL partial unique index (player_id WHERE status='QUEUED')
	for _, entry := range f.entries {
		if entry.PlayerID == input.PlayerID && entry.Status == QueueStatusQueued {
			return nil, ErrPlayerAlreadyInQueue
		}
	}

	ttl := input.TTL
	if ttl == 0 {
		ttl = 5 * time.Minute
	}

	f.nextID++
	id := fmt.Sprintf("queue-entry-%d", f.nextID)
	now := time.Now().UTC()

	q := &QueueEntry{
		ID:          id,
		PlayerID:    input.PlayerID,
		SkillRating: input.SkillRating,
		Region:      input.Region,
		Status:      QueueStatusQueued,
		JoinedAt:    now,
		ExpiresAt:   now.Add(ttl),
	}

	f.entries[id] = q
	return q, nil
}

// GetActiveByPlayerID retrieves a player's currently active queue entry from memory.
func (f *FakeQueueStore) GetActiveByPlayerID(ctx context.Context, playerID string) (*QueueEntry, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ErrToReturn != nil {
		return nil, f.ErrToReturn
	}

	for _, entry := range f.entries {
		if entry.PlayerID == playerID && entry.Status == QueueStatusQueued {
			return entry, nil
		}
	}

	return nil, ErrQueueEntryNotFound
}

// UpdateStatus changes the state of a queue entry in memory.
func (f *FakeQueueStore) UpdateStatus(ctx context.Context, id string, status QueueStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ErrToReturn != nil {
		return f.ErrToReturn
	}

	entry, ok := f.entries[id]
	if !ok {
		return ErrQueueEntryNotFound
	}

	// Modify the pointer directly to update the state in the map
	entry.Status = status
	return nil
}

// ExpireStaleEntries transitions entries in QUEUED status whose ExpiresAt <= cutoff to EXPIRED.
func (f *FakeQueueStore) ExpireStaleEntries(ctx context.Context, cutoff time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ErrToReturn != nil {
		return 0, f.ErrToReturn
	}

	var count int64
	for _, entry := range f.entries {
		if entry.Status == QueueStatusQueued && (entry.ExpiresAt.Before(cutoff) || entry.ExpiresAt.Equal(cutoff)) {
			entry.Status = QueueStatusExpired
			count++
		}
	}
	return count, nil
}

// GetLatestByPlayerID retrieves a player's most recent queue entry from memory.
func (f *FakeQueueStore) GetLatestByPlayerID(ctx context.Context, playerID string) (*QueueEntry, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ErrToReturn != nil {
		return nil, f.ErrToReturn
	}

	var latest *QueueEntry
	for _, entry := range f.entries {
		if entry.PlayerID == playerID {
			if latest == nil || entry.JoinedAt.After(latest.JoinedAt) {
				latest = entry
			}
		}
	}

	if latest == nil {
		return nil, ErrQueueEntryNotFound
	}
	return latest, nil
}
