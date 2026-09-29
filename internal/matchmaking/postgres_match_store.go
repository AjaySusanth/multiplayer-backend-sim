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

func NewHandler(pool *pgxpool.Pool) *PostgresMatchStore {
	return &PostgresMatchStore{
		pool: pool,
	}
}

func (s *PostgresMatchStore) CreateMatch(ctx context.Context,region string, playerIDs []string) (*Match, error) {
	tx,err := s.pool.Begin(ctx)
	if err!=nil {
		return nil, fmt.Errorf("starting a transaction %w",err)
	}

	defer tx.Rollback(ctx)

	m := &Match{
		Region: region,
		Players: make([]MatchPlayer,0,len(playerIDs)),
	}

	matchQuery := `
		INSERT INTO matches (region)
		VALUES ($1)
		RETURNING id, status, created_at
	`
	err = tx.QueryRow(ctx,matchQuery,region).Scan(&m.ID,&m.Status,&m.CreatedAt)

	participantQuery := `
		INSERT INTO match_players (match_id, player_id)
		VALUES ($1, $2)
		RETURNING joined_at
	`

	for _,pID := range playerIDs {
		mp := MatchPlayer{PlayerID: pID}
		err =  tx.QueryRow(ctx,participantQuery,m.ID,pID).Scan(&mp.JoinedAt)
		if err!= nil {
			return nil, fmt.Errorf("inserting match participant %s: %w", pID, err)
		}
		m.Players = append(m.Players, mp)
	}

	if err:= tx.Commit(ctx); err!=nil {
		return nil, fmt.Errorf("committing match transaction: %w", err)
	}
	return m,nil
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