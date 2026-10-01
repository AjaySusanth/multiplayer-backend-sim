package matchmaking

import (
	"time"
)

type MatchStatus string

const (
	MatchStatusCreated   MatchStatus = "CREATED"
	MatchStatusActive    MatchStatus = "ACTIVE"
	MatchStatusCompleted MatchStatus = "COMPLETED"
	MatchStatusCancelled MatchStatus = "CANCELLED"
)

type MatchPlayer struct {
	PlayerID string     `json:"player_id"`
	Team     string     `json:"team,omitempty"`
	JoinedAt time.Time  `json:"joined_at"`
	LeftAt   *time.Time `json:"left_at,omitempty"`
}

type Match struct {
	ID          string        `json:"id"`
	Status      MatchStatus   `json:"status"`
	Region      string        `json:"region"`
	Players     []MatchPlayer `json:"players"`
	CreatedAt   time.Time     `json:"created_at"`
	StartedAt   *time.Time    `json:"started_at,omitempty"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
}