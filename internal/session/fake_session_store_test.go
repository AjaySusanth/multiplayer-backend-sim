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

type FakeMatchChecker struct {
	mu      sync.RWMutex
	members map[string]map[string]bool
	err     error
}

func NewFakeMatchChecker() *FakeMatchChecker {
	return &FakeMatchChecker{
		members: make(map[string]map[string]bool),
	}
}

func (f *FakeMatchChecker) AddMember(matchID, playerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.members[matchID] == nil {
		f.members[matchID] = make(map[string]bool)
	}
	f.members[matchID][playerID] = true
}

func (f *FakeMatchChecker) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *FakeMatchChecker) IsPlayerInMatch(ctx context.Context, matchID, playerID string) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.err != nil {
		return false, f.err
	}
	if f.members[matchID] == nil {
		return false, nil
	}
	return f.members[matchID][playerID], nil
}
