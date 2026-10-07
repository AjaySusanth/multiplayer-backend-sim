package leaderboard

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresLeaderboardStore struct {
	pool *pgxpool.Pool
}

func NewPostgresLeaderboardStore(pool *pgxpool.Pool) *PostgresLeaderboardStore {
	return &PostgresLeaderboardStore{pool: pool}
}

// ErrUnranked is returned when a player has no match results to rank.
var ErrUnranked = errors.New("player is unranked")

func (s *PostgresLeaderboardStore) GetTopPlayers(ctx context.Context, limit int) ([]LeaderboardEntry, error) {
	// CTE (Common Table Expression) to first calculate scores and ranks,
	// then join against the players table for the final output.
	query := `
		WITH ranked_players AS (
			SELECT 
				player_id,
				SUM(score) AS total_score,
				RANK() OVER (ORDER BY SUM(score) DESC) AS rank
			FROM match_results
			GROUP BY player_id
		)
		SELECT 
			r.rank, 
			r.player_id, 
			p.name, 
			p.region, 
			r.total_score
		FROM ranked_players r
		JOIN players p ON p.id = r.player_id
		ORDER BY r.rank ASC
		LIMIT $1
	`

	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("querying top players: %w", err)
	}
	defer rows.Close()

	var entries []LeaderboardEntry
	for rows.Next() {
		var e LeaderboardEntry
		if err := rows.Scan(&e.Rank, &e.PlayerID, &e.PlayerName, &e.Region, &e.TotalScore); err != nil {
			return nil, fmt.Errorf("scanning leaderboard entry: %w", err)
		}
		entries = append(entries, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating leaderboard rows: %w", err)
	}

	if entries == nil {
		return []LeaderboardEntry{}, nil // Return empty slice instead of null JSON
	}
	return entries, nil
}

func (s *PostgresLeaderboardStore) GetPlayerRank(ctx context.Context, playerID uuid.UUID) (*LeaderboardEntry, error) {
	// Reuses the exact same CTE, but filters for a specific player at the end.
	query := `
		WITH ranked_players AS (
			SELECT 
				player_id,
				SUM(score) AS total_score,
				RANK() OVER (ORDER BY SUM(score) DESC) AS rank
			FROM match_results
			GROUP BY player_id
		)
		SELECT 
			r.rank, 
			r.player_id, 
			p.name, 
			p.region, 
			r.total_score
		FROM ranked_players r
		JOIN players p ON p.id = r.player_id
		WHERE r.player_id = $1
	`

	var e LeaderboardEntry
	err := s.pool.QueryRow(ctx, query, playerID).Scan(&e.Rank, &e.PlayerID, &e.PlayerName, &e.Region, &e.TotalScore)
	
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUnranked
		}
		return nil, fmt.Errorf("querying player rank: %w", err)
	}

	return &e, nil
}