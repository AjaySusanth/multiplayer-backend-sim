package matchmaking

import (
	"context"
	"errors"
)

var (
	ErrMatchNotFound        = errors.New("match not found")
	ErrQueueTicketNotQueued = errors.New("one or more players do not have an active queued ticket")
)

type MatchStore interface {
	// CreateMatch inserts a match and assigns the provided players to it transactionally.
	CreateMatch(ctx context.Context, region string, playerIDs []string) (*Match, error)

	// GetByID retrieves a match and its participants by ID.
	GetByID(ctx context.Context, matchID string) (*Match, error)
}
