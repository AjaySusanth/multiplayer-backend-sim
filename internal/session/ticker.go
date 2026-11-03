package session

import "time"

// Ticker defines an injectable tick source so tests can advance ticks deterministically.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealTicker is a production Ticker backed by time.Ticker.
type RealTicker struct {
	ticker *time.Ticker
}

// NewRealTicker creates a RealTicker running at the specified duration.
func NewRealTicker(d time.Duration) *RealTicker {
	return &RealTicker{
		ticker: time.NewTicker(d),
	}
}

func (r *RealTicker) C() <-chan time.Time {
	return r.ticker.C
}

func (r *RealTicker) Stop() {
	r.ticker.Stop()
}

// ManualTicker allows unit and integration tests to trigger ticks synchronously on demand.
type ManualTicker struct {
	ch chan time.Time
}

// NewManualTicker creates a ManualTicker for tests.
func NewManualTicker() *ManualTicker {
	return &ManualTicker{
		ch: make(chan time.Time, 1000),
	}
}

func (m *ManualTicker) C() <-chan time.Time {
	return m.ch
}

func (m *ManualTicker) Stop() {
	// No-op for manual ticker
}

// Tick sends a tick event into the ticker channel.
func (m *ManualTicker) Tick() {
	m.ch <- time.Now()
}
