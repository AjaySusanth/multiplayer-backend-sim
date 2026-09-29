package matchmaking

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"multiplayer-backend-sim/internal/player"
)

// setupTestEnv initializes the handler with all in-memory Fakes.
func setupTestEnv() (http.Handler, *FakeQueueStore, *FakeQueuePublisher, *player.FakePlayerStore) {
	qs := NewFakeQueueStore()
	ms := NewFakeMatchStore()
	pub := NewFakeQueuePublisher()
	ps := player.NewFakePlayerStore()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(ps, qs, ms, pub, logger)

	r := chi.NewRouter()
	handler.RegisterRoutes(r)

	return r, qs, pub, ps
}

func TestHandleJoin(t *testing.T) {
	t.Parallel()

	t.Run("success new ticket", func(t *testing.T) {
		r, _, pub, ps := setupTestEnv()

		// Pre-seed a player in the "database"
		p, _ := ps.Create(context.Background(), player.CreatePlayerInput{
			Name:        "alice",
			SkillRating: 1500,
			Region:      "eu-west",
		})

		reqBody := JoinRequest{PlayerID: p.ID}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/matchmaking/join", bytes.NewReader(bodyBytes))
		rr := httptest.NewRecorder()

		// Dispatch request
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected 201 Created, got %d", rr.Code)
		}

		// Strictly verify the Redis publish side-effect
		if len(pub.JobsPublished) != 1 {
			t.Fatalf("expected 1 job published, got %d", len(pub.JobsPublished))
		}
		if pub.JobsPublished[0].PlayerID != p.ID {
			t.Errorf("expected published job to match player ID")
		}
	})

	t.Run("idempotent join", func(t *testing.T) {
		r, qs, pub, ps := setupTestEnv()

		p, _ := ps.Create(context.Background(), player.CreatePlayerInput{Name: "bob"})

		// Seed an active queue entry to simulate they already clicked Join
		_, _ = qs.Create(context.Background(), CreateQueueEntryInput{PlayerID: p.ID})

		reqBody := JoinRequest{PlayerID: p.ID}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/matchmaking/join", bytes.NewReader(bodyBytes))
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		// Should return 200 OK instead of 201 Created
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rr.Code)
		}

		// The handler shouldn't publish a new duplicate job to Redis
		if len(pub.JobsPublished) != 0 {
			t.Fatalf("expected 0 jobs published for duplicate join, got %d", len(pub.JobsPublished))
		}
	})
}

func TestHandleLeave(t *testing.T) {
	t.Parallel()

	t.Run("success leave", func(t *testing.T) {
		r, qs, _, _ := setupTestEnv()

		// Seed active queue entry
		_, _ = qs.Create(context.Background(), CreateQueueEntryInput{PlayerID: "player-123"})

		reqBody := LeaveRequest{PlayerID: "player-123"}
		bodyBytes, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/api/v1/matchmaking/leave", bytes.NewReader(bodyBytes))
		rr := httptest.NewRecorder()

		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("expected 204 No Content, got %d", rr.Code)
		}

		// Verify status was updated (it should no longer be returned as active)
		updatedEntry, _ := qs.GetActiveByPlayerID(context.Background(), "player-123")
		if updatedEntry != nil {
			t.Errorf("expected no active entry, but got one")
		}
	})
}