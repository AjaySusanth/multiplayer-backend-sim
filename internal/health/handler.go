package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type Handler struct {
	dbPinger Pinger
}

func NewHandler(dbPinger Pinger) *Handler {
	return &Handler{
		dbPinger: dbPinger,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health/live", h.handleLiveness)
	mux.HandleFunc("GET /health/ready", h.handleReadiness)
}

func (h *Handler) handleLiveness(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{
		"status": "UP",
	})
}

func (h *Handler) handleReadiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if h.dbPinger != nil {
		if err := h.dbPinger.Ping(ctx); err != nil {
			h.writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "DOWN",
				"reason": "Database unreachable",
			})
			return
		}
	}
	h.writeJSON(w, http.StatusOK, map[string]string{
		"status":   "UP",
		"database": "UP",
	})
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
