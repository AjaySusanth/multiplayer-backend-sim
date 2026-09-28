package player

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLSTATE code for unique violation in PostgreSQL.
const pgErrUniqueViolation = "23505"

// PostgresPlayerStore implements PlayerStore using pgx/v5 connection pool.
type PostgresPlayerStore struct {
	pool *pgxpool.Pool
}

// NewPostgresPlayerStore creates a new PostgresPlayerStore instance.
func NewPostgresPlayerStore(pool *pgxpool.Pool) *PostgresPlayerStore {
	return &PostgresPlayerStore{
		pool: pool,
	}
}

// Create inserts a new player record into PostgreSQL and returns the fully populated Player entity.
func (s *PostgresPlayerStore) Create(ctx context.Context, input CreatePlayerInput) (*Player, error) {
	query := `
		INSERT INTO players (name, skill_rating, region)
		VALUES ($1, $2, $3)
		RETURNING id, name, skill_rating, region, status, created_at, updated_at
	`

	var p Player
	err := s.pool.QueryRow(ctx, query, input.Name, input.SkillRating, input.Region).Scan(
		&p.ID,
		&p.Name,
		&p.SkillRating,
		&p.Region,
		&p.Status,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgErrUniqueViolation {
			return nil, ErrDuplicatePlayerName
		}
		return nil, fmt.Errorf("inserting player into db: %w", err)
	}

	return &p, nil
}

// GetByID queries a player by UUID from PostgreSQL.
func (s *PostgresPlayerStore) GetByID(ctx context.Context, id string) (*Player, error) {
	query := `
		SELECT id, name, skill_rating, region, status, created_at, updated_at
		FROM players
		WHERE id = $1
	`

	var p Player
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&p.ID,
		&p.Name,
		&p.SkillRating,
		&p.Region,
		&p.Status,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err,pgx.ErrNoRows) {
			return nil, ErrPlayerNotFound
		}
		return nil, fmt.Errorf("querying player by id: %w", err)
	}

	return &p, nil
}

// Delete removes a player record by UUID from PostgreSQL.
func (s *PostgresPlayerStore) Delete(ctx context.Context, id string) error {
	query := `
		DELETE FROM players
		WHERE id = $1
	`

	tag, err := s.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("deleting player from db: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrPlayerNotFound
	}

	return nil
}