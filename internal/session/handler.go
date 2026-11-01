package session

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type Handler struct {
	sessionStore SessionStore
	roomManager  *RoomManager
	logger       *slog.Logger
}

func NewHandler(sessionStore SessionStore, logger *slog.Logger) *Handler {
	return &Handler{
		sessionStore: sessionStore,
		roomManager:  NewRoomManager(context.Background(), logger),
		logger:       logger,
	}
}

func (h *Handler) WithRoomManager(rm *RoomManager) *Handler {
	if rm != nil {
		h.roomManager = rm
	}
	return h
}

func (h *Handler) RoomManager() *RoomManager {
	return h.roomManager
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Get("/ws/session/{session_id}", h.handleWebSocket)
}

func (h *Handler) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "session_id")
	playerID := r.URL.Query().Get("player_id")

	if sessionID == "" || playerID == "" {
		http.Error(w, "missing session_id or player_id", http.StatusBadRequest)
		return
	}
	gameSession, err := h.sessionStore.GetByID(r.Context(), sessionID)
	if err != nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if gameSession.Status != SessionStatusStarting && gameSession.Status != SessionStatusInProgress {
		http.Error(w, "session is no longer active", http.StatusForbidden)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("failed to upgrade websocket", "error", err)
		return
	}

	room := h.roomManager.GetOrCreate(sessionID, gameSession.MatchID)

	client := &Client{
		PlayerID: playerID,
		Send:     make(chan []byte, 256),
	}

	select {
	case room.Register <- client:
	case <-room.Done():
		http.Error(w, "session room is already closed", http.StatusGone)
		conn.Close()
		return
	}

	go client.RunReadPump(conn, room, h.logger)
	go client.RunWritePump(conn)
}
