package result

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	store          ResultStore
	internalSecret string
}

func NewHandler(store ResultStore, internalSecret ...string) *Handler {
	secret := "dev-internal-secret"
	if len(internalSecret) > 0 && internalSecret[0] != "" {
		secret = internalSecret[0]
	}
	return &Handler{
		store:          store,
		internalSecret: secret,
	}
}

func (h *Handler) WithInternalSecret(secret string) *Handler {
	h.internalSecret = secret
	return h
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Post("/api/v1/matches/{match_id}/result", h.SubmitResult)
}

type SubmitResultRequest struct {
	PlayerID uuid.UUID `json:"player_id"`
	Score    int       `json:"score"`
	Result   string    `json:"result"` // e.g., "WIN", "LOSS"
}

func (h *Handler) SubmitResult(w http.ResponseWriter, r *http.Request) {
	// Verify internal authorization secret (endpoint is internal-only)
	secret := r.Header.Get("X-Internal-Secret")
	if h.internalSecret != "" && secret != h.internalSecret {
		http.Error(w, "forbidden: internal-only endpoint", http.StatusForbidden)
		return
	}

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
		if errors.Is(err, ErrResultAlreadySubmitted) {
			http.Error(w, "result already submitted for this match", http.StatusConflict)
			return
		}
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
