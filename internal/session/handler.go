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

// MatchChecker defines the dependency needed to verify player match membership.
type MatchChecker interface {
	IsPlayerInMatch(ctx context.Context, matchID, playerID string) (bool, error)
	GetMatchPlayers(ctx context.Context, matchID string) ([]string, error)
}

type Handler struct {
	sessionStore SessionStore
	matchChecker MatchChecker
	roomManager  *RoomManager
	logger       *slog.Logger
}

func NewHandler(sessionStore SessionStore, matchChecker MatchChecker, logger *slog.Logger) *Handler {
	return &Handler{
		sessionStore: sessionStore,
		matchChecker: matchChecker,
		roomManager:  NewRoomManager(context.Background(), sessionStore, logger),
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

	if h.matchChecker != nil {
		isMember, err := h.matchChecker.IsPlayerInMatch(r.Context(), gameSession.MatchID, playerID)
		if err != nil {
			h.logger.Error("failed to verify match membership", "error", err, "match_id", gameSession.MatchID, "player_id", playerID)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if !isMember {
			h.logger.Warn("player rejected from websocket session: not a member of match", "match_id", gameSession.MatchID, "player_id", playerID)
			http.Error(w, "forbidden: player is not a member of this match", http.StatusForbidden)
			return
		}

		players, err := h.matchChecker.GetMatchPlayers(r.Context(), gameSession.MatchID)
		if err != nil {
			h.logger.Error("failed to retrieve match players", "error", err, "match_id", gameSession.MatchID)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if len(players) != 2 {
			h.logger.Error("invalid match size for tag session", "match_id", gameSession.MatchID, "count", len(players))
			http.Error(w, "bad request: tag simulation requires exactly 2 players", http.StatusBadRequest)
			return
		}
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("failed to upgrade websocket", "error", err)
		return
	}

	var matchPlayers []string
	if h.matchChecker != nil {
		if players, err := h.matchChecker.GetMatchPlayers(r.Context(), gameSession.MatchID); err == nil {
			matchPlayers = players
		}
	}

	room := h.roomManager.GetOrCreate(sessionID, gameSession.MatchID, matchPlayers)

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
