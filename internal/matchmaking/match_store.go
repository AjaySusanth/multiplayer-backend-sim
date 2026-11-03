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

	// IsPlayerInMatch checks whether a player is an assigned participant in the given match.
	IsPlayerInMatch(ctx context.Context, matchID, playerID string) (bool, error)

	// GetMatchPlayers retrieves all player IDs participating in the given match.
	GetMatchPlayers(ctx context.Context, matchID string) ([]string, error)
}
