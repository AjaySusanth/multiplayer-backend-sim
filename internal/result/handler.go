package result

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	store ResultStore
}

func NewHandler(store ResultStore) *Handler {
	return &Handler{store: store}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	// Assuming the router 'r' passed in is already grouped under "/api/v1" 
	// in main.go, as is standard for these patterns.
	r.Post("/matches/{match_id}/result", h.SubmitResult)
}

type SubmitResultRequest struct {
	PlayerID uuid.UUID `json:"player_id"`
	Score    int       `json:"score"`
	Result   string    `json:"result"` // e.g., "WIN", "LOSS"
}

func (h *Handler) SubmitResult(w http.ResponseWriter, r *http.Request) {
	matchIDStr := chi.URLParam(r, "match_id")
	matchID, err := uuid.Parse(matchIDStr)
	if err != nil {
		http.Error(w, "invalid match_id", http.StatusBadRequest)
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		http.Error(w, "Idempotency-Key header required", http.StatusBadRequest)
		return
	}

	var req SubmitResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.PlayerID == uuid.Nil || req.Result == "" {
		http.Error(w, "player_id and result are required", http.StatusBadRequest)
		return
	}

	matchResult := &MatchResult{
		ID:             uuid.New(),
		MatchID:        matchID,
		PlayerID:       req.PlayerID,
		Score:          req.Score,
		Result:         req.Result,
		SubmittedAt:    time.Now().UTC(),
		IdempotencyKey: &idempotencyKey,
	}

	// Pre-encode the success response payload so we can persist it in the idempotency table.
	responseBytes, err := json.Marshal(matchResult)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Attempt the check-and-store idempotency flow via our transactional store.
	cachedRecord, err := h.store.SaveResultIdempotent(r.Context(), matchResult, "POST_MATCH_RESULT", responseBytes)
	if err != nil {
		// (In a real setup we would use log/slog here before returning 500)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// If we got a record back, the insert was a duplicate. Return the cached JSON.
	if cachedRecord != nil {
		w.WriteHeader(http.StatusOK) // 200 OK signals an existing resource was returned
		w.Write(cachedRecord.ResponseJSON)
		return
	}

	// If cachedRecord was nil, it was a fresh insert.
	w.WriteHeader(http.StatusCreated) // 201 Created signals a new resource
	w.Write(responseBytes)
}