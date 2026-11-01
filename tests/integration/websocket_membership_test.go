//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"multiplayer-backend-sim/internal/matchmaking"
	"multiplayer-backend-sim/internal/metrics"
	"multiplayer-backend-sim/internal/session"
)

// TestWebSocket_MatchMembershipHandshake tests the WebSocket "Lobby Door" authorization:
// 1. A legitimate match participant successfully completes the WebSocket handshake (101).
// 2. A registered player not in the match is rejected with HTTP 403 Forbidden.
// 3. An unknown UUID is rejected with HTTP 403 Forbidden.
// 4. A malformed/non-UUID player string is rejected with HTTP 403 Forbidden without database error.
func TestWebSocket_MatchMembershipHandshake(t *testing.T) {
	metrics.Init()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 1. Seed 3 registered players
	p1ID := uuid.New()
	p2ID := uuid.New()
	p3ID := uuid.New() // Stranger: player exists in DB, but not in the match

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, skill_rating, region)
		VALUES 
			($1, 'alice', 1500, 'eu-central'),
			($2, 'bob', 1500, 'eu-central'),
			($3, 'charlie', 1500, 'eu-central')
	`, p1ID, p2ID, p3ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}

	// 2. Enqueue p1 and p2 so CreateMatch passes atomic ticket verification
	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	_, err = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    p1ID.String(),
		SkillRating: 1500,
		Region:      "eu-central",
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 1: %v", err)
	}
	_, err = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    p2ID.String(),
		SkillRating: 1500,
		Region:      "eu-central",
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 2: %v", err)
	}

	// 3. Create match for p1 and p2 only
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	match, err := matchStore.CreateMatch(ctx, "eu-central", []string{p1ID.String(), p2ID.String()})
	if err != nil {
		t.Fatalf("failed to create match: %v", err)
	}

	// 4. Create game session tied to match
	sessionStore := session.NewPostgresSessionStore(harness.PGPool)
	gameSession, err := sessionStore.Create(ctx, match.ID)
	if err != nil {
		t.Fatalf("failed to create game session: %v", err)
	}

	// 4. Mount Session Handler with real PostgresMatchStore
	sessionHandler := session.NewHandler(sessionStore, matchStore, discardLogger)
	r := chi.NewRouter()
	sessionHandler.RegisterRoutes(r)

	server := httptest.NewServer(r)
	defer server.Close()

	baseURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/session/" + gameSession.ID

	// Test 1: Member player 1 connects -> 101 Switching Protocols
	t.Run("member player 1 upgrades to websocket", func(t *testing.T) {
		conn, resp, err := websocket.DefaultDialer.Dial(baseURL+"?player_id="+p1ID.String(), nil)
		if err != nil {
			t.Fatalf("expected successful upgrade for member, got: %v", err)
		}
		defer conn.Close()

		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("expected status 101, got %d", resp.StatusCode)
		}
	})

	// Test 2: Member player 2 connects -> 101 Switching Protocols
	t.Run("member player 2 upgrades to websocket", func(t *testing.T) {
		conn, resp, err := websocket.DefaultDialer.Dial(baseURL+"?player_id="+p2ID.String(), nil)
		if err != nil {
			t.Fatalf("expected successful upgrade for member, got: %v", err)
		}
		defer conn.Close()

		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("expected status 101, got %d", resp.StatusCode)
		}
	})

	// Test 3: Registered player 3 (not in match) connects -> 403 Forbidden
	t.Run("non-member player 3 rejected with 403 Forbidden", func(t *testing.T) {
		conn, resp, err := websocket.DefaultDialer.Dial(baseURL+"?player_id="+p3ID.String(), nil)
		if err == nil {
			conn.Close()
			t.Fatalf("expected upgrade failure for non-member, but connection succeeded")
		}
		if resp == nil {
			t.Fatalf("expected HTTP response, got nil")
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected HTTP 403 Forbidden, got %d", resp.StatusCode)
		}
	})

	// Test 4: Random unknown UUID connects -> 403 Forbidden
	t.Run("unknown player uuid rejected with 403 Forbidden", func(t *testing.T) {
		randomUUID := uuid.New().String()
		conn, resp, err := websocket.DefaultDialer.Dial(baseURL+"?player_id="+randomUUID, nil)
		if err == nil {
			conn.Close()
			t.Fatalf("expected upgrade failure for unknown uuid, but connection succeeded")
		}
		if resp == nil {
			t.Fatalf("expected HTTP response, got nil")
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected HTTP 403 Forbidden, got %d", resp.StatusCode)
		}
	})

	// Test 5: Malformed non-UUID player_id connects -> 403 Forbidden
	t.Run("malformed player id rejected with 403 Forbidden", func(t *testing.T) {
		conn, resp, err := websocket.DefaultDialer.Dial(baseURL+"?player_id=malicious-sql-or-invalid-uuid", nil)
		if err == nil {
			conn.Close()
			t.Fatalf("expected upgrade failure for malformed id, but connection succeeded")
		}
		if resp == nil {
			t.Fatalf("expected HTTP response, got nil")
		}
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected HTTP 403 Forbidden, got %d", resp.StatusCode)
		}
	})
}
