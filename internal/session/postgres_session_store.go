package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSessionStore implements SessionStore using a pgx/v5 connection pool.
type PostgresSessionStore struct {
	pool *pgxpool.Pool
}

// NewPostgresSessionStore creates a new PostgresSessionStore instance.
func NewPostgresSessionStore(pool *pgxpool.Pool) *PostgresSessionStore {
	return &PostgresSessionStore{
		pool: pool,
	}
}

// Create inserts a new game session into the database tied to a specific match.
func (s *PostgresSessionStore) Create(ctx context.Context, matchID string) (*GameSession, error) {
	query := `
		INSERT INTO game_sessions (match_id)
		VALUES ($1)
		RETURNING id, match_id, status, created_at, finished_at
	`

	var gs GameSession
	err := s.pool.QueryRow(ctx, query, matchID).Scan(
		&gs.ID,
		&gs.MatchID,
		&gs.Status,
		&gs.CreatedAt,
		&gs.FinishedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("inserting game session: %w", err)
	}

	return &gs, nil
}

// GetByID retrieves a game session by its ID.
func (s *PostgresSessionStore) GetByID(ctx context.Context, sessionID string) (*GameSession, error) {
	query := `
		SELECT id, match_id, status, created_at, finished_at
		FROM game_sessions
		WHERE id = $1
	`

	var gs GameSession
	err := s.pool.QueryRow(ctx, query, sessionID).Scan(
		&gs.ID,
		&gs.MatchID,
		&gs.Status,
		&gs.CreatedAt,
		&gs.FinishedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSessionNotFound
		}
		return nil, fmt.Errorf("querying game session: %w", err)
	}

	return &gs, nil
}

// UpdateStatus changes the lifecycle state of a game session.
func (s *PostgresSessionStore) UpdateStatus(ctx context.Context, sessionID string, status SessionStatus) error {
	var query string
	
	// If the game is ending, we automatically record the timestamp using PostgreSQL's NOW()
	if status == SessionStatusFinished || status == SessionStatusAborted {
		query = `
			UPDATE game_sessions
			SET status = $2, finished_at = NOW()
			WHERE id = $1
		`
	} else {
		query = `
			UPDATE game_sessions
			SET status = $2
			WHERE id = $1
		`
	}

	tag, err := s.pool.Exec(ctx, query, sessionID, status)
	if err != nil {
		return fmt.Errorf("updating game session status: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrSessionNotFound
	}

	return nil
}