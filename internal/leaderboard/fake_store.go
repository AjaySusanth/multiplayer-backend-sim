package leaderboard

import (
	"context"

	"github.com/google/uuid"
)

// FakeLeaderboardStore provides an in-memory mock of the leaderboard for HTTP tests.
type FakeLeaderboardStore struct {
	// TopPlayers holds a pre-sorted slice of entries to return for GetTopPlayers.
	TopPlayers []LeaderboardEntry
	
	// PlayerRanks maps a PlayerID to their specific rank entry.
	PlayerRanks map[uuid.UUID]*LeaderboardEntry
	
	// Err allows tests to simulate database failures.
	Err error
}

func NewFakeLeaderboardStore() *FakeLeaderboardStore {
	return &FakeLeaderboardStore{
		TopPlayers:  make([]LeaderboardEntry, 0),
		PlayerRanks: make(map[uuid.UUID]*LeaderboardEntry),
	}
}

func (s *FakeLeaderboardStore) GetTopPlayers(ctx context.Context, limit int) ([]LeaderboardEntry, error) {
	if s.Err != nil {
		return nil, s.Err
	}

	// Slice the pre-seeded data up to the requested limit
	if limit > len(s.TopPlayers) {
		return s.TopPlayers, nil
	}
	return s.TopPlayers[:limit], nil
}

func (s *FakeLeaderboardStore) GetPlayerRank(ctx context.Context, playerID uuid.UUID) (*LeaderboardEntry, error) {
	if s.Err != nil {
		return nil, s.Err
	}

	entry, ok := s.PlayerRanks[playerID]
	if !ok {
		return nil, ErrUnranked
	}
	return entry, nil
}