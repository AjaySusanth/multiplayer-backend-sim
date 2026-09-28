package player

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

type Handler struct {
	store  PlayerStore
	logger *slog.Logger
}

func NewHandler(store PlayerStore, logger *slog.Logger) *Handler {
	return &Handler{
		store:  store,
		logger: logger,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/players", h.handleCreate)
	mux.HandleFunc("GET /api/v1/players/{id}", h.handleGet)
	mux.HandleFunc("DELETE /api/v1/players/{id}", h.handleDelete)
}

func (h *Handler) handleCreate(w http.ResponseWriter, r *http.Request) {
	var input CreatePlayerInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid json payload")
		return
	}

	p, err := h.store.Create(r.Context(), input)
	if err != nil {
		if errors.Is(err, ErrDuplicatePlayerName) {
			h.writeError(w, http.StatusConflict, err.Error())
			return
		}
		h.logger.Error("failed to create player", "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	h.writeJSON(w, http.StatusCreated, p)
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing player id")
		return
	}
	p, err := h.store.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrPlayerNotFound) {
			h.writeError(w, http.StatusNotFound, err.Error())
			return
		}
		h.logger.Error("failed to get player", "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
		return

	}
	h.writeJSON(w, http.StatusOK, p)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		h.writeError(w, http.StatusBadRequest, "missing player id")
		return
	}
	err := h.store.Delete(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrPlayerNotFound) {
			h.writeError(w, http.StatusNotFound, err.Error())
			return
		}
		h.logger.Error("failed to delete player", "error", err, "id", id)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Error("failed to encode json response", "error", err)
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}
