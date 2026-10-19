package matchmaking

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)



type PostgresMatchStore struct {
	pool *pgxpool.Pool
}

func NewPostgresMatchStore(pool *pgxpool.Pool) *PostgresMatchStore {
	return &PostgresMatchStore{
		pool: pool,
	}
}

var ErrPlayerAlreadyInMatch = errors.New("one or more players are already in an active match")

func (s *PostgresMatchStore) CreateMatch(ctx context.Context, region string, playerIDs []string) (*Match, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting a transaction %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Idempotency & Collision Check:
	// Check if any player is already in an active match.
	activeCheckQuery := `
		SELECT mp.match_id, mp.player_id
		FROM match_players mp
		JOIN matches m ON m.id = mp.match_id
		WHERE mp.player_id = ANY($1) 
		  AND m.status IN ('CREATED', 'IN_PROGRESS')
		FOR UPDATE OF m
	`
	rows, err := tx.Query(ctx, activeCheckQuery, playerIDs)
	if err != nil {
		return nil, fmt.Errorf("checking active match for players: %w", err)
	}
	defer rows.Close()

	existingMatches := make(map[string]map[string]bool)
	for rows.Next() {
		var mID, pID string
		if err := rows.Scan(&mID, &pID); err != nil {
			return nil, fmt.Errorf("scanning active match check: %w", err)
		}
		if existingMatches[mID] == nil {
			existingMatches[mID] = make(map[string]bool)
		}
		existingMatches[mID][pID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating active match rows: %w", err)
	}

	// If this exact group of players is already in the same active match,
	// return that existing match (idempotent replay of a crashed/retried job).
	for mID, matchedPlayerMap := range existingMatches {
		if len(matchedPlayerMap) == len(playerIDs) {
			// All players are in this match already — return existing match
			tx.Rollback(ctx)
			return s.GetByID(ctx, mID)
		}
		// Otherwise, a player is in a different match
		return nil, ErrPlayerAlreadyInMatch
	}

	// 2. No active match exists for these players — create a new one
	m := &Match{
		Region:  region,
		Players: make([]MatchPlayer, 0, len(playerIDs)),
	}

	matchQuery := `
		INSERT INTO matches (region)
		VALUES ($1)
		RETURNING id, status, created_at
	`
	err = tx.QueryRow(ctx, matchQuery, region).Scan(&m.ID, &m.Status, &m.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("inserting match: %w", err)
	}

	participantQuery := `
		INSERT INTO match_players (match_id, player_id)
		VALUES ($1, $2)
		RETURNING joined_at
	`
	for _, pID := range playerIDs {
		mp := MatchPlayer{PlayerID: pID}
		err = tx.QueryRow(ctx, participantQuery, m.ID, pID).Scan(&mp.JoinedAt)
		if err != nil {
			return nil, fmt.Errorf("inserting match participant %s: %w", pID, err)
		}
		m.Players = append(m.Players, mp)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing match transaction: %w", err)
	}
	return m, nil
}

func (s *PostgresMatchStore) GetByID (ctx context.Context,matchID string) (*Match,error) {
	matchQuery := `
		SELECT id, status, region, created_at, started_at, completed_at
		FROM matches
		WHERE id = $1
	`
	var m Match
	err := s.pool.QueryRow(ctx, matchQuery, matchID).Scan(
		&m.ID,
		&m.Status,
		&m.Region,
		&m.CreatedAt,
		&m.StartedAt,
		&m.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrMatchNotFound
		}
		return nil, fmt.Errorf("querying match: %w", err)
	}

	playersQuery := `
		SELECT player_id, team, joined_at, left_at
		FROM match_players
		WHERE match_id = $1
	`

	rows,err := s.pool.Query(ctx,playersQuery,matchID)
	if err!= nil {
		return nil, fmt.Errorf("querying match players: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var mp MatchPlayer
		var team *string //*string means "string or nil"

		if err:= rows.Scan(&mp.PlayerID, &team, &mp.JoinedAt, &mp.LeftAt); err != nil {
			return nil, fmt.Errorf("scanning match player: %w", err)
		}

		if team != nil {
			mp.Team = *team
		}
		m.Players = append(m.Players, mp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating match players: %w", err)
	}
	return &m,nil
}