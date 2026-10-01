package matchmaking

import (
	"context"
	"errors"
)

// ErrMatchNotFound is returned when querying for a match ID that does not exist.
var ErrMatchNotFound = errors.New("match not found")

// MatchStore defines the persistence interface for creating and fetching matches.
type MatchStore interface {
	// CreateMatch inserts a match and assigns the provided players to it transactionally.
	CreateMatch(ctx context.Context, region string, playerIDs []string) (*Match, error)
	
	// GetByID fetches a match and all of its associated players.
	GetByID(ctx context.Context, matchID string) (*Match, error)
}