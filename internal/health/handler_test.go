package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockPinger struct {
	err error
}

func (m *mockPinger) Ping(ctx context.Context) error {
	return m.err
}

func TestHealthEndpoints(t *testing.T) {
	t.Parallel()

	t.Run("Liveness returns 200 OK", func(t *testing.T) {
		h := NewHandler(nil)
		req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		rr := httptest.NewRecorder()

		h.handleLiveness(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, rr.Code)
		}
	})

	t.Run("Readiness returns 200 OK when DB ping succeeds", func(t *testing.T) {
		pinger := &mockPinger{err: nil}
		h := NewHandler(pinger)
		req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
		rr := httptest.NewRecorder()

		h.handleReadiness(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, rr.Code)
		}
	})

	t.Run("Readiness returns 503 Service Unavailable when DB ping fails", func(t *testing.T) {
		pinger := &mockPinger{err: errors.New("connection failed")}
		h := NewHandler(pinger)
		req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
		rr := httptest.NewRecorder()

		h.handleReadiness(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, rr.Code)
		}
	})
}
