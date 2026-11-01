package session

import (
	"context"
	"log/slog"
	"sync"
)

// RoomManager owns the lifecycle and contexts of all active in-memory game rooms.
type RoomManager struct {
	mu      sync.RWMutex
	rooms   map[string]*Room
	cancels map[string]context.CancelFunc
	rootCtx context.Context
	cancel  context.CancelFunc
	logger  *slog.Logger
}

// NewRoomManager initializes a RoomManager with its own root context.
func NewRoomManager(ctx context.Context, logger *slog.Logger) *RoomManager {
	if ctx == nil {
		ctx = context.Background()
	}
	mCtx, mCancel := context.WithCancel(ctx)
	return &RoomManager{
		rooms:   make(map[string]*Room),
		cancels: make(map[string]context.CancelFunc),
		rootCtx: mCtx,
		cancel:  mCancel,
		logger:  logger,
	}
}

// GetOrCreate retrieves an active room by session ID or starts a new one with its own context.
func (m *RoomManager) GetOrCreate(sessionID, matchID string) *Room {
	m.mu.Lock()
	defer m.mu.Unlock()

	if room, exists := m.rooms[sessionID]; exists {
		return room
	}

	roomCtx, roomCancel := context.WithCancel(m.rootCtx)
	room := NewRoom(sessionID, matchID, m.logger, func(id string) {
		m.Remove(id)
	})

	m.rooms[sessionID] = room
	m.cancels[sessionID] = roomCancel

	go room.Run(roomCtx)
	return room
}

// Get retrieves an active room by session ID.
func (m *RoomManager) Get(sessionID string) (*Room, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	room, exists := m.rooms[sessionID]
	return room, exists
}

// Remove cleans up a room entry and cancels its internal context.
func (m *RoomManager) Remove(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cancel, exists := m.cancels[sessionID]; exists {
		cancel()
		delete(m.cancels, sessionID)
	}
	delete(m.rooms, sessionID)
}

// ActiveCount returns the number of currently running game rooms.
func (m *RoomManager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}

// Close terminates all active game rooms cleanly.
func (m *RoomManager) Close() {
	m.cancel()
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, cancel := range m.cancels {
		cancel()
		delete(m.cancels, id)
	}
	m.rooms = make(map[string]*Room)
}
