package session

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
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
	Send chan []byte
}

type PlayerCommand struct {
	PlayerID string
	Action string
}

type Room struct {
	ID string
	MatchID string

	state GameState
	stateMu sync.RWMutex
	
	Clients map[string]*Client

	Register chan *Client
	Unregister chan *Client
	Commands chan PlayerCommand

	logger *slog.Logger
}

func NewRoom(sessionID, matchID string, logger *slog.Logger) *Room {
	return &Room{
		ID:         sessionID,
		MatchID:    matchID,
		state:      GameState{Players: make(map[string]*Position)},
		Clients:    make(map[string]*Client),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Commands:   make(chan PlayerCommand, 100), // Buffer up to 100 fast keystrokes
		logger:     logger,
	}
}

func (r *Room) Run(ctx context.Context) {
	r.logger.Info("game room started", "session_id", r.ID)
	ticker  := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <- ctx.Done():
			r.logger.Info("game room shutting down", "session_id", r.ID)
			return
		
		case client := <-r.Register:
			r.Clients[client.PlayerID] = client
			r.stateMu.Lock()

			if _,exists := r.state.Players[client.PlayerID]; !exists {
				r.state.Players[client.PlayerID] = &Position{
					X: 0,
					Y: 0,
				}
			}

			r.stateMu.Unlock()
			r.logger.Info("player joined arena", "player_id", client.PlayerID)

		case client := <-r.Unregister:
			if _,ok:= r.Clients[client.PlayerID]; ok{
				delete(r.Clients,client.PlayerID)
				r.logger.Info("player left arena", "player_id", client.PlayerID)

				// If both players left, the arena is empty. Shut it down.
				if len(r.Clients) == 0 {
					r.logger.Info("room empty, shutting down", "session_id", r.ID)
					return
				}
			}

		case cmd := <-r.Commands:
			r.stateMu.Lock()
			pos,exists := r.state.Players[cmd.PlayerID]
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
			stateJSON,err := json.Marshal(r.state)
			r.stateMu.Unlock()

			if err != nil {
				r.logger.Error("failed to marshal game state", "error", err)
				continue
			}

			for _,client :=range r.Clients{
				select {
				case client.Send <- stateJSON:
					// Success! Sent to their browser.
				default:
					// IMPORTANT: If a player is playing on a terrible 3G mobile connection, 
					// their 'Send' pipe will get clogged. If we wait for them, the whole server 
					// will freeze for the other player! This 'default' block ensures we just skip 
					// sending them this specific frame so the game stays smooth for everyone else.
				}
			}

		

	
		}
	}
}