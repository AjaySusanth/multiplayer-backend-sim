package matchmaking

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// DefaultExpiryInterval is the default frequency for sweeping stale queue tickets.
const DefaultExpiryInterval = 2 * time.Second

// QueueExpiryWorker runs a periodic background loop that identifies tickets in QUEUED
// status whose expires_at timestamp has passed, and transitions them to EXPIRED.
type QueueExpiryWorker struct {
	queueStore QueueStore
	logger     *slog.Logger
	interval   time.Duration
}

// NewQueueExpiryWorker initializes a QueueExpiryWorker instance.
func NewQueueExpiryWorker(qs QueueStore, logger *slog.Logger, interval time.Duration) *QueueExpiryWorker {
	if interval <= 0 {
		interval = DefaultExpiryInterval
	}
	return &QueueExpiryWorker{
		queueStore: qs,
		logger:     logger,
		interval:   interval,
	}
}

// Sweep executes a single atomic expiration pass against the queue store.
func (w *QueueExpiryWorker) Sweep(ctx context.Context, cutoff time.Time) (int64, error) {
	count, err := w.queueStore.ExpireStaleEntries(ctx, cutoff)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			w.logger.Error("failed to expire stale queue entries", "error", err)
		}
		return 0, err
	}
	if count > 0 {
		w.logger.Info("expired stale queue tickets", "count", count)
	}
	return count, nil
}

// Run starts the ticker loop and sweeps for stale tickets until the context is cancelled.
// It shuts down cleanly upon context cancellation without leaking goroutines.
func (w *QueueExpiryWorker) Run(ctx context.Context) {
	w.logger.Info("queue expiry worker started", "interval", w.interval)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("queue expiry worker shutting down gracefully")
			return
		case now := <-ticker.C:
			_, _ = w.Sweep(ctx, now)
		}
	}
}
