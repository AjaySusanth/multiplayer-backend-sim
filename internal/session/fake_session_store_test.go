package session

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type FakeSessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*GameSession
	nextID   int
}

func NewFakeSessionStore() *FakeSessionStore {
	return &FakeSessionStore{
		sessions: make(map[string]*GameSession),
	}
}

func (f *FakeSessionStore) Create(ctx context.Context, matchID string) (*GameSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextID++
	id := fmt.Sprintf("session-%d", f.nextID)
	sess := &GameSession{
		ID:        id,
		MatchID:   matchID,
		Status:    SessionStatusStarting,
		CreatedAt: time.Now().UTC(),
	}
	f.sessions[id] = sess
	return sess, nil
}

func (f *FakeSessionStore) GetByID(ctx context.Context, sessionID string) (*GameSession, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	sess, ok := f.sessions[sessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return sess, nil
}

func (f *FakeSessionStore) UpdateStatus(ctx context.Context, sessionID string, status SessionStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	sess, ok := f.sessions[sessionID]
	if !ok {
		return ErrSessionNotFound
	}
	sess.Status = status
	if status == SessionStatusFinished || status == SessionStatusAborted {
		now := time.Now().UTC()
		sess.FinishedAt = &now
	}
	return nil
}

func (f *FakeSessionStore) SeedSession(s *GameSession) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessions[s.ID] = s
}
