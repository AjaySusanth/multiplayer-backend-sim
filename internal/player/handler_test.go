package player

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupTestEnv initializes the handler with a FakePlayerStore and a discarded logger.
func setupTestEnv() (*Handler, *FakePlayerStore) {
	store := NewFakePlayerStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(store, logger), store
}

func TestHandleCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupStore     func(*FakePlayerStore)
		payload        any
		expectedStatus int
	}{
		{
			name:           "success",
			setupStore:     func(s *FakePlayerStore) {},
			payload:        CreatePlayerInput{Name: "alice", SkillRating: 1200, Region: "us-east"},
			expectedStatus: http.StatusCreated,
		},
		{
			name: "duplicate name",
			setupStore: func(s *FakePlayerStore) {
				// Pre-seed the fake store to simulate an existing player
				_, _ = s.Create(context.Background(), CreatePlayerInput{Name: "alice"})
			},
			payload:        CreatePlayerInput{Name: "alice"},
			expectedStatus: http.StatusConflict,
		},
		{
			name: "store error",
			setupStore: func(s *FakePlayerStore) {
				s.ErrToReturn = errors.New("database down")
			},
			payload:        CreatePlayerInput{Name: "bob"},
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store := setupTestEnv()
			tt.setupStore(store)

			var body bytes.Buffer
			_ = json.NewEncoder(&body).Encode(tt.payload)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/players", &body)
			rr := httptest.NewRecorder()

			h.handleCreate(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d. Body: %s", tt.expectedStatus, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestHandleGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupStore     func(*FakePlayerStore) string // returns the ID to request
		expectedStatus int
	}{
		{
			name: "success",
			setupStore: func(s *FakePlayerStore) string {
				p, _ := s.Create(context.Background(), CreatePlayerInput{Name: "alice"})
				return p.ID
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "not found",
			setupStore: func(s *FakePlayerStore) string {
				return "non-existent-id"
			},
			expectedStatus: http.StatusNotFound,
		},
		{
			name: "store error",
			setupStore: func(s *FakePlayerStore) string {
				p, _ := s.Create(context.Background(), CreatePlayerInput{Name: "alice"})
				s.ErrToReturn = errors.New("database down")
				return p.ID
			},
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, store := setupTestEnv()
			reqID := tt.setupStore(store)

			req := httptest.NewRequest(http.MethodGet, "/api/v1/players/"+reqID, nil)
			// Go 1.22 allows manually setting routing PathValues on the request for testing
			req.SetPathValue("id", reqID)

			rr := httptest.NewRecorder()
			h.handleGet(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}
