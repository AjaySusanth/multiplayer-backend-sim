package liveops

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func setupTestServer() (*FakeLiveOpsStore, *chi.Mux) {
	store := NewFakeLiveOpsStore()
	handler := NewHandler(store)

	r := chi.NewRouter()
	handler.RegisterRoutes(r)

	return store, r
}

func TestCreateConfiguration(t *testing.T) {
	store, r := setupTestServer()

	tests := []struct {
		name           string
		payload        string
		expectedStatus int
		simulateErr    error
	}{
		{
			name: "success - valid draft configuration",
			payload: `{
				"version": 1,
				"actor_id": "admin-1",
				"configuration": {
					"max_skill_difference": 500,
					"max_queue_wait_seconds": 60,
					"match_size": 2
				}
			}`,
			expectedStatus: http.StatusCreated,
		},
		{
			name: "error - invalid payload validation (negative skill diff)",
			payload: `{
				"version": 2,
				"actor_id": "admin-1",
				"configuration": {
					"max_skill_difference": -10,
					"max_queue_wait_seconds": 60,
					"match_size": 2
				}
			}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "error - invalid match size (less than 2)",
			payload: `{
				"version": 3,
				"actor_id": "admin-1",
				"configuration": {
					"max_skill_difference": 500,
					"max_queue_wait_seconds": 60,
					"match_size": 1
				}
			}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "error - missing actor_id",
			payload: `{
				"version": 4,
				"configuration": {
					"max_skill_difference": 500,
					"max_queue_wait_seconds": 60,
					"match_size": 2
				}
			}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name: "error - duplicate version",
			payload: `{
				"version": 1,
				"actor_id": "admin-1",
				"configuration": {
					"max_skill_difference": 500,
					"max_queue_wait_seconds": 60,
					"match_size": 2
				}
			}`,
			expectedStatus: http.StatusConflict,
		},
		{
			name: "error - internal database failure",
			payload: `{
				"version": 5,
				"actor_id": "admin-1",
				"configuration": {
					"max_skill_difference": 500,
					"max_queue_wait_seconds": 60,
					"match_size": 2
				}
			}`,
			expectedStatus: http.StatusInternalServerError,
			simulateErr:    errors.New("connection failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.SimulatedErr = tt.simulateErr

			req := httptest.NewRequest(http.MethodPost, "/api/v1/liveops/configurations", bytes.NewBufferString(tt.payload))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Fatalf("expected status %d, got %d. Body: %s", tt.expectedStatus, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestActivateAndRollbackConfiguration(t *testing.T) {
	store, r := setupTestServer()

	// Seed version 1 as ACTIVE and version 2 as DRAFT
	now := time.Now().UTC()
	rawJSON := json.RawMessage(`{"max_skill_difference": 500}`)
	store.Configs[1] = &ConfigVersion{
		ID:                uuid.New(),
		Version:           1,
		ConfigurationJSON: rawJSON,
		Status:            StatusActive,
		CreatedBy:         "admin-1",
		CreatedAt:         now,
		ActivatedAt:       &now,
	}
	store.Configs[2] = &ConfigVersion{
		ID:                uuid.New(),
		Version:           2,
		ConfigurationJSON: rawJSON,
		Status:            StatusDraft,
		CreatedBy:         "admin-1",
		CreatedAt:         now,
	}

	t.Run("activate version 2 - success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/liveops/configurations/2/activate", nil)
		req.Header.Set("X-Actor-ID", "admin-super")
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
		}

		if store.Configs[2].Status != StatusActive {
			t.Errorf("expected version 2 status to be ACTIVE, got %s", store.Configs[2].Status)
		}
		if store.Configs[1].Status != StatusArchived {
			t.Errorf("expected version 1 status to be ARCHIVED, got %s", store.Configs[1].Status)
		}
	})

	t.Run("activate version 2 again - conflict", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/liveops/configurations/2/activate", nil)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusConflict {
			t.Fatalf("expected status %d, got %d", http.StatusConflict, rr.Code)
		}
	})

	t.Run("rollback to version 1 - success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/liveops/configurations/1/rollback", nil)
		req.Header.Set("X-Actor-ID", "admin-rollback")
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
		}

		if store.Configs[1].Status != StatusActive {
			t.Errorf("expected version 1 status to be ACTIVE after rollback, got %s", store.Configs[1].Status)
		}
		if store.Configs[2].Status != StatusArchived {
			t.Errorf("expected version 2 status to be ARCHIVED after rollback, got %s", store.Configs[2].Status)
		}
	})

	t.Run("activate non-existent version - not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/liveops/configurations/999/activate", nil)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected status %d, got %d", http.StatusNotFound, rr.Code)
		}
	})
}

func TestGetActiveAndAuditLogs(t *testing.T) {
	store, r := setupTestServer()

	// Initially no active config
	t.Run("get active - not found initially", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/liveops/configurations/active", nil)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected status %d, got %d", http.StatusNotFound, rr.Code)
		}
	})

	// Seed an active config and audit entry
	now := time.Now().UTC()
	store.Configs[1] = &ConfigVersion{
		ID:                uuid.New(),
		Version:           1,
		ConfigurationJSON: json.RawMessage(`{"match_size": 2}`),
		Status:            StatusActive,
		CreatedBy:         "admin",
		CreatedAt:         now,
		ActivatedAt:       &now,
	}
	store.AuditLogs = append(store.AuditLogs, AuditLog{
		ID:           uuid.New(),
		ActorID:      "admin",
		Action:       ActionConfigCreated,
		ResourceType: "LIVEOPS_CONFIG",
		ResourceID:   "1",
		CreatedAt:    now,
	})

	t.Run("get active - success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/liveops/configurations/active", nil)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
		}
	})

	t.Run("list audit logs - success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/liveops/audit-logs?limit=10", nil)
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
		}

		var logs []AuditLog
		if err := json.NewDecoder(rr.Body).Decode(&logs); err != nil {
			t.Fatalf("failed to decode audit logs: %v", err)
		}
		if len(logs) != 1 {
			t.Errorf("expected 1 audit log, got %d", len(logs))
		}
	})
}