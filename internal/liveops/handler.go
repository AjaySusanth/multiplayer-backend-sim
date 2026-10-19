package liveops

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	store LiveOpsStore
}

func NewHandler(store LiveOpsStore) *Handler {
	return &Handler{store: store}
}

// RegisterRoutes wires the LiveOps and Audit endpoints into the Chi router.
func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Route("/api/v1/liveops", func(r chi.Router) {
		r.Post("/configurations", h.CreateConfiguration)
		r.Get("/configurations", h.ListConfigurations)
		r.Get("/configurations/active", h.GetActiveConfiguration)
		r.Post("/configurations/{version}/activate", h.ActivateConfiguration)
		r.Post("/configurations/{version}/rollback", h.RollbackConfiguration)
		r.Get("/audit-logs", h.ListAuditLogs)
	})
}

type CreateConfigRequest struct {
	Version           int                      `json:"version"`
	Configuration     MatchmakingConfigPayload `json:"configuration"`
	ActorID           string                   `json:"actor_id"`
}

func (h *Handler) CreateConfiguration(w http.ResponseWriter, r *http.Request) {
	var req CreateConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Version <= 0 {
		http.Error(w, "version must be a positive integer", http.StatusBadRequest)
		return
	}
	if req.ActorID == "" {
		http.Error(w, "actor_id is required", http.StatusBadRequest)
		return
	}

	// Validate the configuration payload schema
	if err := req.Configuration.Validate(); err != nil {
		http.Error(w, "invalid configuration payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	rawJSON, err := json.Marshal(req.Configuration)
	if err != nil {
		http.Error(w, "failed to serialize configuration payload", http.StatusInternalServerError)
		return
	}

	now := time.Now().UTC()
	configID := uuid.New()
	cfg := &ConfigVersion{
		ID:                configID,
		Version:           req.Version,
		ConfigurationJSON: rawJSON,
		Status:            StatusDraft,
		CreatedBy:         req.ActorID,
		CreatedAt:         now,
	}

	audit := &AuditLog{
		ID:           uuid.New(),
		ActorID:      req.ActorID,
		Action:       ActionConfigCreated,
		ResourceType: "LIVEOPS_CONFIG",
		ResourceID:   strconv.Itoa(req.Version),
		MetadataJSON: rawJSON,
		CreatedAt:    now,
	}

	if err := h.store.CreateVersion(r.Context(), cfg, audit); err != nil {
		if errors.Is(err, ErrVersionExists) {
			http.Error(w, "configuration version already exists", http.StatusConflict)
			return
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(cfg)
}

func (h *Handler) ActivateConfiguration(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.Atoi(chi.URLParam(r, "version"))
	if err != nil || version <= 0 {
		http.Error(w, "invalid version parameter", http.StatusBadRequest)
		return
	}

	actorID := r.Header.Get("X-Actor-ID")
	if actorID == "" {
		actorID = "system-operator" // Fallback default when auth header is omitted
	}

	if err := h.store.ActivateVersion(r.Context(), version, actorID); err != nil {
		switch {
		case errors.Is(err, ErrVersionNotFound):
			http.Error(w, "configuration version not found", http.StatusNotFound)
		case errors.Is(err, ErrAlreadyActive):
			http.Error(w, "configuration version is already active", http.StatusConflict)
		default:
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "configuration activated successfully",
	})
}

func (h *Handler) RollbackConfiguration(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.Atoi(chi.URLParam(r, "version"))
	if err != nil || version <= 0 {
		http.Error(w, "invalid version parameter", http.StatusBadRequest)
		return
	}

	actorID := r.Header.Get("X-Actor-ID")
	if actorID == "" {
		actorID = "system-operator"
	}

	if err := h.store.RollbackVersion(r.Context(), version, actorID); err != nil {
		switch {
		case errors.Is(err, ErrVersionNotFound):
			http.Error(w, "configuration version not found", http.StatusNotFound)
		case errors.Is(err, ErrAlreadyActive):
			http.Error(w, "configuration version is already active", http.StatusConflict)
		default:
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "configuration rolled back successfully",
	})
}

func (h *Handler) GetActiveConfiguration(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.store.GetActive(r.Context())
	if err != nil {
		if errors.Is(err, ErrNoActiveConfig) {
			http.Error(w, "no active configuration found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cfg)
}

func (h *Handler) ListConfigurations(w http.ResponseWriter, r *http.Request) {
	configs, err := h.store.ListConfigurations(r.Context())
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(configs)
}

func (h *Handler) ListAuditLogs(w http.ResponseWriter, r *http.Request) {
	limit := 50
	limitStr := r.URL.Query().Get("limit")
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			if parsed > 100 {
				parsed = 100
			}
			limit = parsed
		}
	}

	logs, err := h.store.ListAuditLogs(r.Context(), limit)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}