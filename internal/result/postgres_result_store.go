package result

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresResultStore struct {
	pool *pgxpool.Pool
}

func NewPostgresResultStore(pool *pgxpool.Pool) *PostgresResultStore {
	return &PostgresResultStore{pool: pool}
}

func (s *PostgresResultStore) SaveResultIdempotent(ctx context.Context, res *MatchResult, operation string, responseJSON json.RawMessage) (*IdempotencyRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Attempt to insert the idempotency key first.
	// We rely purely on the Postgres unique constraint to prevent duplicates.
	_, err = tx.Exec(ctx, `
		INSERT INTO idempotency_keys (idempotency_key, operation, response_json)
		VALUES ($1, $2, $3)
	`, *res.IdempotencyKey, operation, responseJSON)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			// The transaction is now in an aborted state due to the error.
			// We roll it back explicitly before querying the existing record.
			tx.Rollback(ctx)

			var cachedJSON []byte
			selectErr := s.pool.QueryRow(ctx, `
				SELECT response_json 
				FROM idempotency_keys 
				WHERE idempotency_key = $1 AND operation = $2
			`, *res.IdempotencyKey, operation).Scan(&cachedJSON)

			if selectErr != nil {
				return nil, fmt.Errorf("fetching duplicate idempotency response: %w", selectErr)
			}

			return &IdempotencyRecord{
				IdempotencyKey: *res.IdempotencyKey,
				Operation:      operation,
				ResponseJSON:   cachedJSON,
			}, nil
		}
		return nil, fmt.Errorf("inserting idempotency key: %w", err)
	}

	// 2. If the idempotency key was successfully inserted, it's a fresh request.
	// Proceed to insert the match result within the same transaction.
	_, err = tx.Exec(ctx, `
		INSERT INTO match_results (id, match_id, player_id, score, result, submitted_at, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, res.ID, res.MatchID, res.PlayerID, res.Score, res.Result, res.SubmittedAt, res.IdempotencyKey)
	
	if err != nil {
		return nil, fmt.Errorf("inserting match result: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}

	return nil, nil // nil record means a fresh, successful insert
}