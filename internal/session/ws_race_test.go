package session

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"multiplayer-backend-sim/internal/metrics"
)

func TestWebSocket_TwoClientsConcurrencyAndMapCleanup(t *testing.T) {
	metrics.Init()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeStore := NewFakeSessionStore()

	sessionID := "session-concurrent-1"
	matchID := "match-concurrent-1"

	fakeStore.SeedSession(&GameSession{
		ID:        sessionID,
		MatchID:   matchID,
		Status:    SessionStatusStarting,
		CreatedAt: time.Now().UTC(),
	})

	fakeMatchChecker := NewFakeMatchChecker()
	fakeMatchChecker.AddMember(matchID, "player-1")
	fakeMatchChecker.AddMember(matchID, "player-2")

	roomManager := NewRoomManager(context.Background(), fakeStore, nil, logger)
	handler := NewHandler(fakeStore, fakeMatchChecker, nil, logger).WithRoomManager(roomManager)

	r := chi.NewRouter()
	handler.RegisterRoutes(r)

	server := httptest.NewServer(r)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/session/" + sessionID

	// 1. Connect Client 1
	conn1, _, err := websocket.DefaultDialer.Dial(wsURL+"?player_id=player-1", nil)
	if err != nil {
		t.Fatalf("failed to dial client 1: %v", err)
	}

	// 2. Connect Client 2
	conn2, _, err := websocket.DefaultDialer.Dial(wsURL+"?player_id=player-2", nil)
	if err != nil {
		conn1.Close()
		t.Fatalf("failed to dial client 2: %v", err)
	}

	// 3. Verify RoomManager registered exactly 1 active room
	if roomManager.ActiveCount() != 1 {
		t.Fatalf("expected 1 active room in RoomManager, got %d", roomManager.ActiveCount())
	}

	// 4. Send movement command from Client 1 and Client 2 concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		cmd := map[string]string{"action": "UP"}
		bytes, _ := json.Marshal(cmd)
		_ = conn1.WriteMessage(websocket.TextMessage, bytes)
	}()

	go func() {
		defer wg.Done()
		cmd := map[string]string{"action": "RIGHT"}
		bytes, _ := json.Marshal(cmd)
		_ = conn2.WriteMessage(websocket.TextMessage, bytes)
	}()

	wg.Wait()

	// 5. Read state updates from both clients
	readState := func(conn *websocket.Conn) GameState {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("failed to read game state from websocket: %v", err)
		}
		var state GameState
		if err := json.Unmarshal(msg, &state); err != nil {
			t.Fatalf("failed to unmarshal game state: %v", err)
		}
		return state
	}

	state1 := readState(conn1)
	state2 := readState(conn2)

	if len(state1.Players) == 0 || len(state2.Players) == 0 {
		t.Fatalf("expected non-empty game state broadcast to both clients")
	}

	// 6. Prove client 1 disconnect does NOT terminate room for client 2
	// (Guarantees room lifecycle is decoupled from client 1's request context)
	if err := conn1.Close(); err != nil {
		t.Errorf("error closing client 1 connection: %v", err)
	}

	// Client 2 continues to send input and receive frames
	cmd2 := map[string]string{"action": "DOWN"}
	c2Bytes, _ := json.Marshal(cmd2)
	if err := conn2.WriteMessage(websocket.TextMessage, c2Bytes); err != nil {
		t.Fatalf("client 2 failed to send command after client 1 disconnected: %v", err)
	}

	stateAfterP1Left := readState(conn2)
	if _, exists := stateAfterP1Left.Players["player-2"]; !exists {
		t.Fatalf("expected player-2 to still be active in arena after player-1 disconnected")
	}

	// 7. Disconnect Client 2; room must cleanly detect empty arena, shutdown, and unregister
	if err := conn2.Close(); err != nil {
		t.Errorf("error closing client 2 connection: %v", err)
	}

	// Poll until RoomManager cleans up the room from its active map
	deadline := time.Now().Add(3 * time.Second)
	cleanedUp := false
	for time.Now().Before(deadline) {
		if roomManager.ActiveCount() == 0 {
			cleanedUp = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !cleanedUp {
		t.Fatalf("expected RoomManager active count to drop to 0 after all clients disconnected, got %d", roomManager.ActiveCount())
	}
}
