package leaderboard

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func TestGetTopPlayers(t *testing.T) {
	store := NewFakeLeaderboardStore()
	handler := NewHandler(store)

	r := chi.NewRouter()
	handler.RegisterRoutes(r)

	// Pre-seed the fake store with some data
	store.TopPlayers = []LeaderboardEntry{
		{Rank: 1, PlayerName: "Ajay", TotalScore: 100},
		{Rank: 2, PlayerName: "Bob", TotalScore: 50},
	}

	tests := []struct {
		name           string
		query          string
		expectedStatus int
		simulateErr    error
	}{
		{
			name:           "success - default limit",
			query:          "",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "success - explicit valid limit",
			query:          "?limit=1",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "error - invalid limit format",
			query:          "?limit=abc",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "error - internal database error",
			query:          "",
			expectedStatus: http.StatusInternalServerError,
			simulateErr:    errors.New("database connection refused"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.Err = tt.simulateErr // Inject error if specified by the test case

			req := httptest.NewRequest("GET", "/leaderboards"+tt.query, nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestGetPlayerRank(t *testing.T) {
	store := NewFakeLeaderboardStore()
	handler := NewHandler(store)

	r := chi.NewRouter()
	handler.RegisterRoutes(r)

	playerID := uuid.New()
	
	// Pre-seed a specific rank for our known playerID
	store.PlayerRanks[playerID] = &LeaderboardEntry{
		Rank: 1, PlayerID: playerID, TotalScore: 100,
	}

	tests := []struct {
		name           string
		playerID       string
		expectedStatus int
		simulateErr    error
	}{
		{
			name:           "success - player found",
			playerID:       playerID.String(),
			expectedStatus: http.StatusOK,
		},
		{
			name:           "error - invalid uuid format",
			playerID:       "not-a-uuid",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "error - unranked player (not found)",
			playerID:       uuid.New().String(), // A fresh UUID that isn't in our map
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "error - internal database error",
			playerID:       playerID.String(),
			expectedStatus: http.StatusInternalServerError,
			simulateErr:    errors.New("database connection refused"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.Err = tt.simulateErr

			req := httptest.NewRequest("GET", "/leaderboards/"+tt.playerID+"/rank", nil)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, rr.Code)
			}
		})
	}
}