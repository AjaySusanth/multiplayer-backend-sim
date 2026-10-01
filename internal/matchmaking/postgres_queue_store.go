package matchmaking

import (
	"context"
	"errors"
	"fmt"
	"time"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)


const pgErrUniqueViolation = "23505"

type PostgresQueueStore struct {
	pool *pgxpool.Pool
}

func NewPostgresQueueStore(pool *pgxpool.Pool) *PostgresQueueStore {
	return  &PostgresQueueStore{
		pool: pool,
	}
}

func (s *PostgresQueueStore) Create(ctx context.Context,input CreateQueueEntryInput) (*QueueEntry,error) {
	expiresAt :=time.Now().UTC().Add(5*time.Minute)
	query:= `INSERT INTO queue_entries (player_id, skill_rating, region, expires_at)
		VALUES ($1, $2, $3, $4)
		RETURNING id, player_id, skill_rating, region, status, joined_at, matched_at, expires_at`

	var q QueueEntry
	err:=s.pool.QueryRow(ctx,query,input.PlayerID,input.SkillRating, input.Region, expiresAt).Scan(
		&q.ID,
		&q.PlayerID,
		&q.SkillRating,
		&q.Region,
		&q.Status,
		&q.JoinedAt,
		&q.MatchedAt,
		&q.ExpiresAt,
	)
	if err!= nil {
		var pgErr *pgconn.PgError
		if errors.As(err,&pgErr) && pgErr.Code == pgErrUniqueViolation {
			return nil, ErrPlayerAlreadyInQueue
		}

		return nil, fmt.Errorf("inserting queue entry: %w", err)
	}
	return &q,nil
}

func (s *PostgresQueueStore) GetActiveByPlayerID(ctx context.Context,playerID string) (*QueueEntry,error) {
	query := `
		SELECT id, player_id, skill_rating, region, status, joined_at, matched_at, expires_at FROM queue_entries
		WHERE player_id = $1 and status = "QUEUED"
	`

	var q QueueEntry

	err := s.pool.QueryRow(ctx,query,playerID).Scan(
		&q.ID,
		&q.PlayerID,
		&q.SkillRating,
		&q.Region,
		&q.Status,
		&q.JoinedAt,
		&q.MatchedAt,
		&q.ExpiresAt,
	)

	if err!=nil {
		if errors.Is(err,pgx.ErrNoRows) {
			return nil,ErrQueueEntryNotFound
		}
		return nil, fmt.Errorf("querying active queue entry: %w", err)
	}
	return &q,nil
}

func (s *PostgresQueueStore) UpdateStatus(ctx context.Context, id string, status QueueStatus) error {
	query := `
		UPDATE queue_entries
		SET status = $2
		WHERE id = $1
	`
	tag, err := s.pool.Exec(ctx, query, id, status)
	if err != nil {
		return fmt.Errorf("updating queue entry status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrQueueEntryNotFound
	}
	return nil
}
