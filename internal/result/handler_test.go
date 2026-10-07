package result

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func TestSubmitResult(t *testing.T) {
	store := NewFakeResultStore()
	handler := NewHandler(store)

	r := chi.NewRouter()
	r.Post("/api/v1/matches/{match_id}/result", handler.SubmitResult)

	matchID := uuid.New()
	playerID := uuid.New()
	idemKey := "test-idem-key-123"

	validBody := map[string]interface{}{
		"player_id": playerID.String(),
		"score":     100,
		"result":    "WIN",
	}
	bodyBytes, _ := json.Marshal(validBody)

	tests := []struct {
		name           string
		matchID        string
		idempotencyKey string
		body           []byte
		expectedStatus int
	}{
		{
			name:           "success - fresh submission",
			matchID:        matchID.String(),
			idempotencyKey: idemKey,
			body:           bodyBytes,
			expectedStatus: http.StatusCreated, // 201
		},
		{
			name:           "success - duplicate idempotent submission",
			matchID:        matchID.String(),
			idempotencyKey: idemKey, // Same key hits the duplicate check logic
			body:           bodyBytes,
			expectedStatus: http.StatusOK, // 200
		},
		{
			name:           "error - missing idempotency key",
			matchID:        matchID.String(),
			idempotencyKey: "",
			body:           bodyBytes,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "error - invalid match id",
			matchID:        "not-a-uuid",
			idempotencyKey: "fresh-key-456",
			body:           bodyBytes,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/v1/matches/"+tt.matchID+"/result", bytes.NewReader(tt.body))
			if tt.idempotencyKey != "" {
				req.Header.Set("Idempotency-Key", tt.idempotencyKey)
			}
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			// Dispatch via Chi router to ensure path param binding works properly
			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, rr.Code)
			}
		})
	}
}