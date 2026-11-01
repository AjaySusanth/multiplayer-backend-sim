package session

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"multiplayer-backend-sim/internal/metrics"
)

type Position struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type GameState struct {
	Players map[string]*Position `json:"players"`
}

type Client struct {
	PlayerID string
	Send     chan []byte
}

type PlayerCommand struct {
	PlayerID string
	Action   string
}

type Room struct {
	ID      string
	MatchID string

	state   GameState
	stateMu sync.RWMutex

	Clients map[string]*Client

	Register   chan *Client
	Unregister chan *Client
	Commands   chan PlayerCommand

	done    chan struct{}
	onClose func(roomID string)
	logger  *slog.Logger
}

func NewRoom(sessionID, matchID string, logger *slog.Logger, onClose func(string)) *Room {
	return &Room{
		ID:         sessionID,
		MatchID:    matchID,
		state:      GameState{Players: make(map[string]*Position)},
		Clients:    make(map[string]*Client),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Commands:   make(chan PlayerCommand, 100), // Buffer up to 100 fast keystrokes
		done:       make(chan struct{}),
		onClose:    onClose,
		logger:     logger,
	}
}

// Done returns a channel that is closed when the Room finishes execution.
func (r *Room) Done() <-chan struct{} {
	return r.done
}

func (r *Room) Run(ctx context.Context) {
	metrics.ActiveMatches.Inc()
	defer func() {
		metrics.ActiveMatches.Dec()
		close(r.done)
		if r.onClose != nil {
			r.onClose(r.ID)
		}
	}()

	r.logger.Info("game room started", "session_id", r.ID)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("game room shutting down", "session_id", r.ID)
			return

		case client := <-r.Register:
			r.Clients[client.PlayerID] = client
			r.stateMu.Lock()

			if _, exists := r.state.Players[client.PlayerID]; !exists {
				r.state.Players[client.PlayerID] = &Position{
					X: 0,
					Y: 0,
				}
			}

			r.stateMu.Unlock()
			r.logger.Info("player joined arena", "player_id", client.PlayerID)

		case client := <-r.Unregister:
			if _, ok := r.Clients[client.PlayerID]; ok {
				delete(r.Clients, client.PlayerID)
				r.logger.Info("player left arena", "player_id", client.PlayerID)

				// If all players left, the arena is empty. Shut it down.
				if len(r.Clients) == 0 {
					r.logger.Info("room empty, shutting down", "session_id", r.ID)
					return
				}
			}

		case cmd := <-r.Commands:
			r.stateMu.Lock()
			pos, exists := r.state.Players[cmd.PlayerID]
			if exists {
				switch cmd.Action {
				case "UP":
					pos.Y++
				case "DOWN":
					pos.Y--
				case "LEFT":
					pos.X--
				case "RIGHT":
					pos.X++
				}
			}
			r.stateMu.Unlock()

		case <-ticker.C:
			r.stateMu.RLock()
			stateJSON, err := json.Marshal(r.state)
			r.stateMu.RUnlock()

			if err != nil {
				r.logger.Error("failed to marshal game state", "error", err)
				continue
			}

			for _, client := range r.Clients {
				select {
				case client.Send <- stateJSON:
					// Sent successfully to client write pump buffer
				default:
					// Drop frame if client send buffer is full to prevent freezing room for others
				}
			}
		}
	}
}
