package session

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/metrics"
	"multiplayer-backend-sim/internal/result"
)

// GameState represents the broadcast snapshot of the arena.
type GameState struct {
	Type           string                     `json:"type,omitempty"`
	Status         SessionStatus              `json:"status,omitempty"`
	Tick           int                        `json:"tick,omitempty"`
	TicksRemaining int                        `json:"ticks_remaining,omitempty"`
	CurrentItID    string                     `json:"current_it_id,omitempty"`
	Players        map[string]*PlayerSimState `json:"players"`
	Outcome        *GameOutcome               `json:"outcome,omitempty"`
}

type Client struct {
	PlayerID string
	Send     chan []byte
}

type PlayerCommand struct {
	PlayerID string
	DX       float64
	DY       float64
	Action   string
}

type Room struct {
	ID           string
	MatchID      string
	sessionStore SessionStore
	resultStore  result.ResultStore
	matchPlayers []string

	game    *TagGame
	ticker  Ticker
	stateMu sync.RWMutex

	Clients map[string]*Client

	Register   chan *Client
	Unregister chan *Client
	Commands   chan PlayerCommand

	done    chan struct{}
	onClose func(roomID string)
	logger  *slog.Logger
}

func NewRoom(sessionID, matchID string, matchPlayers []string, sessionStore SessionStore, resultStore result.ResultStore, logger *slog.Logger, onClose func(string)) *Room {
	r := &Room{
		ID:           sessionID,
		MatchID:      matchID,
		sessionStore: sessionStore,
		resultStore:  resultStore,
		matchPlayers: matchPlayers,
		Clients:      make(map[string]*Client),
		Register:     make(chan *Client),
		Unregister:   make(chan *Client),
		Commands:     make(chan PlayerCommand, 100),
		done:         make(chan struct{}),
		onClose:      onClose,
		logger:       logger,
	}

	if len(matchPlayers) == 2 {
		r.game = NewTagGame(matchPlayers[0], matchPlayers[1])
	}
	return r
}

// WithTicker injects a custom or manual ticker for testing.
func (r *Room) WithTicker(t Ticker) *Room {
	r.ticker = t
	return r
}

// Done returns a channel that is closed when the Room finishes execution.
func (r *Room) Done() <-chan struct{} {
	return r.done
}

// Game returns the underlying TagGame state (for inspection in tests).
func (r *Room) Game() *TagGame {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return r.game
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

	if r.ticker == nil {
		r.ticker = NewRealTicker(100 * time.Millisecond)
	}
	defer r.ticker.Stop()

	status := SessionStatusStarting
	lobbyTicks := 0
	currentTick := 0

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("game room shutting down", "session_id", r.ID)
			return

		case client := <-r.Register:
			r.Clients[client.PlayerID] = client
			r.logger.Info("player joined arena", "player_id", client.PlayerID, "client_count", len(r.Clients))

			// If both players connected and session was in STARTING state:
			if len(r.Clients) == 2 && status == SessionStatusStarting {
				status = SessionStatusInProgress
				if r.sessionStore != nil {
					if err := r.sessionStore.UpdateStatus(ctx, r.ID, SessionStatusInProgress); err != nil {
						r.logger.Error("failed to update session status to IN_PROGRESS", "error", err)
					}
				}

				r.stateMu.Lock()
				if r.game == nil {
					clientIDs := make([]string, 0, 2)
					for id := range r.Clients {
						clientIDs = append(clientIDs, id)
					}
					r.game = NewTagGame(clientIDs[0], clientIDs[1])
				}
				r.stateMu.Unlock()

				r.broadcastFrame("round_start", status, currentTick)
				r.logger.Info("both players connected, tag round starting", "session_id", r.ID)
			}

		case client := <-r.Unregister:
			if _, ok := r.Clients[client.PlayerID]; ok {
				delete(r.Clients, client.PlayerID)
				r.logger.Info("player left arena", "player_id", client.PlayerID)

				// If match was IN_PROGRESS, mid-round disconnect triggers forfeit!
				if status == SessionStatusInProgress {
					r.stateMu.Lock()
					var outcome *GameOutcome
					if r.game != nil {
						outcome = r.game.Forfeit(client.PlayerID)
					}
					r.stateMu.Unlock()

					status = SessionStatusFinished
					if r.sessionStore != nil {
						if err := r.sessionStore.UpdateStatus(ctx, r.ID, SessionStatusFinished); err != nil {
							r.logger.Error("failed to update session status to FINISHED on forfeit", "error", err)
						}
					}

					r.broadcastFrame("game_over", status, currentTick)
					if outcome != nil {
						r.submitResults(ctx, outcome)
						r.logger.Info("match forfeit processed", "leaver", client.PlayerID, "winner", outcome.WinnerID)
					}
					return
				}

				// If room was STARTING and became empty, shutdown
				if len(r.Clients) == 0 && status == SessionStatusStarting {
					r.logger.Info("room empty in starting state, shutting down", "session_id", r.ID)
					return
				}
			}

		case cmd := <-r.Commands:
			r.stateMu.Lock()
			if r.game != nil && status == SessionStatusInProgress {
				r.game.ApplyDirection(cmd.PlayerID, Vector{X: cmd.DX, Y: cmd.DY})
			}
			r.stateMu.Unlock()

		case <-r.ticker.C():
			if status == SessionStatusStarting {
				lobbyTicks++
				if lobbyTicks >= LobbyTimeoutTicks { // 150 ticks = 15s at 10Hz
					r.logger.Warn("lobby connection timeout (15s elapsed), aborting session", "session_id", r.ID)
					status = SessionStatusAborted
					if r.sessionStore != nil {
						if err := r.sessionStore.UpdateStatus(ctx, r.ID, SessionStatusAborted); err != nil {
							r.logger.Error("failed to update session status to ABORTED", "error", err)
						}
					}
					return
				}
				continue
			}

			if status == SessionStatusInProgress {
				currentTick++
				r.stateMu.Lock()
				isFinished := r.game.Tick()
				r.stateMu.Unlock()

				r.broadcastFrame("state", status, currentTick)

				if isFinished {
					status = SessionStatusFinished
					if r.sessionStore != nil {
						if err := r.sessionStore.UpdateStatus(ctx, r.ID, SessionStatusFinished); err != nil {
							r.logger.Error("failed to update session status to FINISHED", "error", err)
						}
					}
					r.broadcastFrame("game_over", status, currentTick)
					if r.game.Outcome != nil {
						r.submitResults(ctx, r.game.Outcome)
						r.logger.Info("tag match completed normally", "winner", r.game.Outcome.WinnerID, "loser", r.game.Outcome.LoserID)
					}
					return
				}
			}
		}
	}
}

func (r *Room) broadcastFrame(frameType string, status SessionStatus, tick int) {
	r.stateMu.RLock()
	if r.game == nil {
		r.stateMu.RUnlock()
		return
	}

	frame := GameState{
		Type:           frameType,
		Status:         status,
		Tick:           tick,
		TicksRemaining: r.game.TicksRemaining,
		CurrentItID:    r.game.CurrentItID,
		Players:        r.game.Players,
		Outcome:        r.game.Outcome,
	}
	r.stateMu.RUnlock()

	stateJSON, err := json.Marshal(frame)
	if err != nil {
		r.logger.Error("failed to marshal frame broadcast", "error", err)
		return
	}

	for _, client := range r.Clients {
		select {
		case client.Send <- stateJSON:
		default:
			// Non-blocking drop if client write buffer is full
		}
	}
}

func (r *Room) submitResults(ctx context.Context, outcome *GameOutcome) {
	if r.resultStore == nil || outcome == nil {
		return
	}

	matchUUID, err := uuid.Parse(r.MatchID)
	if err != nil {
		matchUUID = uuid.NewMD5(uuid.NameSpaceDNS, []byte(r.MatchID))
	}

	winnerUUID, err := uuid.Parse(outcome.WinnerID)
	if err != nil {
		winnerUUID = uuid.NewMD5(uuid.NameSpaceDNS, []byte(outcome.WinnerID))
	}

	loserUUID, err := uuid.Parse(outcome.LoserID)
	if err != nil {
		loserUUID = uuid.NewMD5(uuid.NameSpaceDNS, []byte(outcome.LoserID))
	}

	now := time.Now().UTC()

	// Deterministic idempotency keys: match:<matchID>:result:<playerID>
	winnerIdemKey := fmt.Sprintf("match:%s:result:%s", r.MatchID, outcome.WinnerID)
	loserIdemKey := fmt.Sprintf("match:%s:result:%s", r.MatchID, outcome.LoserID)

	winnerResult := &result.MatchResult{
		ID:             uuid.New(),
		MatchID:        matchUUID,
		PlayerID:       winnerUUID,
		Score:          outcome.Scores[outcome.WinnerID],
		Result:         "WIN",
		SubmittedAt:    now,
		IdempotencyKey: &winnerIdemKey,
	}

	loserResult := &result.MatchResult{
		ID:             uuid.New(),
		MatchID:        matchUUID,
		PlayerID:       loserUUID,
		Score:          outcome.Scores[outcome.LoserID],
		Result:         "LOSS",
		SubmittedAt:    now,
		IdempotencyKey: &loserIdemKey,
	}

	respWinner, _ := json.Marshal(winnerResult)
	if _, err := r.resultStore.SaveResultIdempotent(ctx, winnerResult, "SESSION_OUTCOME", respWinner); err != nil {
		r.logger.Error("failed to submit winner match result", "error", err, "player_id", outcome.WinnerID)
	}

	respLoser, _ := json.Marshal(loserResult)
	if _, err := r.resultStore.SaveResultIdempotent(ctx, loserResult, "SESSION_OUTCOME", respLoser); err != nil {
		r.logger.Error("failed to submit loser match result", "error", err, "player_id", outcome.LoserID)
	}

	r.logger.Info("server-authoritative match results submitted", "match_id", r.MatchID, "winner", outcome.WinnerID, "loser", outcome.LoserID)
}
