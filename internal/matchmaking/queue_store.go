package matchmaking

import (
	"context"
	"errors"
	"time"
)

var (
	ErrQueueEntryNotFound   = errors.New("queue entry not found")
	ErrPlayerAlreadyInQueue = errors.New("player is already in the active matchmaking queue")
)

type CreateQueueEntryInput struct {
	PlayerID    string
	SkillRating int
	Region      string
	TTL         time.Duration // optional custom wait limit; defaults to 5 minutes if <= 0
}

type QueueStore interface {
	Create(ctx context.Context, input CreateQueueEntryInput) (*QueueEntry, error)
	GetActiveByPlayerID(ctx context.Context, playerID string) (*QueueEntry, error)
	GetLatestByPlayerID(ctx context.Context, playerID string) (*QueueEntry, error)
	UpdateStatus(ctx context.Context, id string, status QueueStatus) error
	ExpireStaleEntries(ctx context.Context, cutoff time.Time) (int64, error)
}
