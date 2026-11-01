package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"multiplayer-backend-sim/internal/metrics"
)

func TestWebSocket_MembershipVerification(t *testing.T) {
	metrics.Init()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	const (
		validSessionID = "sess-door-1"
		finishedSessID = "sess-finished-1"
		validMatchID   = "match-door-1"
		memberPlayerID = "player-member-1"
		strangerPlayer = "player-stranger-99"
	)

	fakeStore := NewFakeSessionStore()
	fakeStore.SeedSession(&GameSession{
		ID:        validSessionID,
		MatchID:   validMatchID,
		Status:    SessionStatusStarting,
		CreatedAt: time.Now().UTC(),
	})
	fakeStore.SeedSession(&GameSession{
		ID:        finishedSessID,
		MatchID:   validMatchID,
		Status:    SessionStatusFinished,
		CreatedAt: time.Now().UTC(),
	})

	fakeChecker := NewFakeMatchChecker()
	fakeChecker.AddMember(validMatchID, memberPlayerID)

	roomManager := NewRoomManager(context.Background(), logger)
	handler := NewHandler(fakeStore, fakeChecker, logger).WithRoomManager(roomManager)

	r := chi.NewRouter()
	handler.RegisterRoutes(r)

	server := httptest.NewServer(r)
	defer server.Close()

	baseURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/session/"

	tests := []struct {
		name           string
		sessionID      string
		playerID       string
		setupMock      func()
		cleanupMock    func()
		expectedStatus int
		expectUpgrade  bool
	}{
		{
			name:           "valid member succeeds with 101 Switching Protocols",
			sessionID:      validSessionID,
			playerID:       memberPlayerID,
			expectedStatus: http.StatusSwitchingProtocols,
			expectUpgrade:  true,
		},
		{
			name:           "stranger/non-member is rejected with 403 Forbidden",
			sessionID:      validSessionID,
			playerID:       strangerPlayer,
			expectedStatus: http.StatusForbidden,
			expectUpgrade:  false,
		},
		{
			name:           "missing player_id query param rejected with 400 Bad Request",
			sessionID:      validSessionID,
			playerID:       "",
			expectedStatus: http.StatusBadRequest,
			expectUpgrade:  false,
		},
		{
			name:           "non-existent session rejected with 404 Not Found",
			sessionID:      "non-existent-session-id",
			playerID:       memberPlayerID,
			expectedStatus: http.StatusNotFound,
			expectUpgrade:  false,
		},
		{
			name:           "finished session rejected with 403 Forbidden",
			sessionID:      finishedSessID,
			playerID:       memberPlayerID,
			expectedStatus: http.StatusForbidden,
			expectUpgrade:  false,
		},
		{
			name:      "checker failure returns 500 Internal Server Error",
			sessionID: validSessionID,
			playerID:  memberPlayerID,
			setupMock: func() {
				fakeChecker.SetError(errors.New("db connection failure"))
			},
			cleanupMock: func() {
				fakeChecker.SetError(nil)
			},
			expectedStatus: http.StatusInternalServerError,
			expectUpgrade:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupMock != nil {
				tt.setupMock()
			}
			if tt.cleanupMock != nil {
				defer tt.cleanupMock()
			}

			url := baseURL + tt.sessionID
			if tt.playerID != "" {
				url += "?player_id=" + tt.playerID
			}

			conn, resp, err := websocket.DefaultDialer.Dial(url, nil)
			if tt.expectUpgrade {
				if err != nil {
					t.Fatalf("expected successful upgrade, got error: %v", err)
				}
				defer conn.Close()
				if resp.StatusCode != http.StatusSwitchingProtocols {
					t.Errorf("expected status %d, got %d", http.StatusSwitchingProtocols, resp.StatusCode)
				}
			} else {
				if err == nil {
					conn.Close()
					t.Fatalf("expected error upgrading websocket, but connection succeeded")
				}
				if resp == nil {
					t.Fatalf("expected HTTP response, got nil (err=%v)", err)
				}
				if resp.StatusCode != tt.expectedStatus {
					t.Errorf("expected HTTP status %d, got %d", tt.expectedStatus, resp.StatusCode)
				}
			}
		})
	}
}
