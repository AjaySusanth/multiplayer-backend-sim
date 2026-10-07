package leaderboard

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	store LeaderboardStore
}

func NewHandler(store LeaderboardStore) *Handler {
	return &Handler{store: store}
}

// RegisterRoutes wires the leaderboard endpoints into the provided Chi router.
func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Get("/leaderboards", h.GetTopPlayers)
	r.Get("/leaderboards/{player_id}/rank", h.GetPlayerRank)
}

func (h *Handler) GetTopPlayers(w http.ResponseWriter, r *http.Request) {
	limit := 10 // Safe default

	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 {
			http.Error(w, "invalid limit parameter", http.StatusBadRequest)
			return
		}
		
		// Hard-cap the limit to prevent clients from requesting massive result sets
		if parsedLimit > 100 {
			parsedLimit = 100
		}
		limit = parsedLimit
	}

	entries, err := h.store.GetTopPlayers(r.Context(), limit)
	if err != nil {
		// (In a real app we'd log the underlying err via slog here)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

func (h *Handler) GetPlayerRank(w http.ResponseWriter, r *http.Request) {
	playerIDStr := chi.URLParam(r, "player_id")
	playerID, err := uuid.Parse(playerIDStr)
	if err != nil {
		http.Error(w, "invalid player_id", http.StatusBadRequest)
		return
	}

	entry, err := h.store.GetPlayerRank(r.Context(), playerID)
	if err != nil {
		// Map the domain-specific ErrUnranked to a 404 HTTP response
		if errors.Is(err, ErrUnranked) {
			http.Error(w, "player is unranked", http.StatusNotFound)
			return
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}