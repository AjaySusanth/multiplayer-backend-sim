package matchmaking

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"github.com/go-chi/chi/v5"
	"multiplayer-backend-sim/internal/player"
)

type Handler struct {
	playerStore player.PlayerStore
	matchStore MatchStore
	queueStore QueueStore
	publisher QueuePublisher
	logger *slog.Logger
}

func NewHandler(ps player.PlayerStore,qs QueueStore,ms MatchStore,pub QueuePublisher,logger *slog.Logger) *Handler{
	return &Handler{ 
		playerStore: ps,
		matchStore: ms,
		queueStore: qs,
		publisher: pub,
		logger: logger,
	}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Post("/api/v1/matchmaking/join", h.handleJoin)
	r.Post("/api/v1/matchmaking/leave", h.handleLeave)
	r.Get("/api/v1/matchmaking/status/{player_id}", h.handleStatus)
}

type JoinRequest struct {
	PlayerID string `json:"player_id"`
}

func (h *Handler) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err!=nil {
		h.writeError(w,http.StatusBadRequest,"invalid request payload")
		return
	}
	if req.PlayerID == "" {
		h.writeError(w, http.StatusBadRequest, "player_id is required")
		return
	}

	p,err := h.playerStore.GetByID(r.Context(),req.PlayerID)
	if err!=nil {
		if errors.Is(err,player.ErrPlayerNotFound) {
			h.writeError(w,http.StatusNotFound,"player not found")
			return
		}
		h.logger.Error("failed to fetch player","error",err,"player id",req.PlayerID)
		h.writeError(w,http.StatusInternalServerError,"internal server error")
		return 
	}
	entry,err:=h.queueStore.Create(r.Context(),CreateQueueEntryInput{
		PlayerID: p.ID,
		SkillRating: p.SkillRating,
		Region: p.Region,
	})

	// Idempotency: If the database rejects it due to the active-queue unique constraint,
	// fetch and return the existing active ticket.
	if errors.Is(err, ErrPlayerAlreadyInQueue) {
		existingEntry, getErr := h.queueStore.GetActiveByPlayerID(r.Context(), p.ID)
		if getErr != nil {
			h.logger.Error("failed to fetch existing queue entry", "error", getErr, "player_id", p.ID)
			h.writeError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		// 200 OK indicates they were already queued.
		h.writeJSON(w, http.StatusOK, existingEntry)
		return
	}

	if err != nil {
		h.logger.Error("failed to create queue entry", "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	// Publish to redis stream
	job:= MatchMakingJob{
		PlayerID: p.ID,
		SkillRating: p.SkillRating,
		Region: p.Region,
	}
	if err:= h.publisher.Publish(r.Context(),job); err!=nil{

		h.logger.Error("failed to publish matchmaking job to redis", "error", err, "player_id", p.ID)
	}
	h.writeJSON(w, http.StatusCreated, entry)
}

type LeaveRequest struct {
	PlayerID string `json:"player_id"`
}

func (h *Handler) handleLeave(w http.ResponseWriter, r *http.Request) {
	var req LeaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request payload")
		return
	}
	activeEntry, err := h.queueStore.GetActiveByPlayerID(r.Context(), req.PlayerID)
	if err != nil {
		if errors.Is(err, ErrQueueEntryNotFound) {
			// Idempotency: If they aren't in the queue, leaving is a successful no-op.
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.logger.Error("failed to fetch active queue entry for leave", "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	// Update ticket status to CANCELLED
	if err := h.queueStore.UpdateStatus(r.Context(), activeEntry.ID, QueueStatusCancelled); err != nil {
		h.logger.Error("failed to cancel queue entry", "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleStatus(w http.ResponseWriter, r *http.Request) {
	playerID := chi.URLParam(r, "player_id")
	if playerID == "" {
		h.writeError(w, http.StatusBadRequest, "missing player id")
		return
	}
	entry, err := h.queueStore.GetActiveByPlayerID(r.Context(), playerID)
	if err != nil {
		if errors.Is(err, ErrQueueEntryNotFound) {
			h.writeError(w, http.StatusNotFound, "player is not actively queued")
			return
		}
		h.logger.Error("failed to fetch queue status", "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	h.writeJSON(w, http.StatusOK, entry)
}


func (h *Handler) writeJSON(w http.ResponseWriter,status int,data any) {
	w.Header().Set("Content-Type","application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

