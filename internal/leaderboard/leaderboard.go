package leaderboard

import (
	"context"

	"github.com/google/uuid"
)

type LeaderboardEntry struct {
	Rank       int       `json:"rank"`
	PlayerID   uuid.UUID `json:"player_id"`
	PlayerName string    `json:"player_name"` // Requires a JOIN with the players table
	Region     string    `json:"region"`      // Requires a JOIN with the players table
	TotalScore int       `json:"total_score"`
}

// LeaderboardStore defines the data access contract for the leaderboard.

type LeaderboardStore interface {
	// GetTopPlayers returns the top N ranked players, ordered by rank ascending.
	GetTopPlayers(ctx context.Context, limit int) ([]LeaderboardEntry, error)

	// GetPlayerRank returns the specific global rank and score for a single player.
	GetPlayerRank(ctx context.Context, playerID uuid.UUID) (*LeaderboardEntry, error)
}