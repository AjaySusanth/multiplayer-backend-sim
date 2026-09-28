package matchmaking

import (
	"context"
	"errors"
)

var (
	ErrQueueEntryNotFound = errors.New("queue entry not found")
	ErrPlayerAlreadyInQueue = errors.New("player is already in the active matchmaking queue")
)

type CreateQueueEntryInput struct {
	PlayerID    string
	SkillRating int
	Region      string
}

type QueueStore interface {
	Create(ctx context.Context, input CreateQueueEntryInput) (*QueueEntry, error)
	GetActiveByPlayerID(ctx context.Context, playerID string) (*QueueEntry, error)
	UpdateStatus(ctx context.Context, id string, status QueueStatus) error
}