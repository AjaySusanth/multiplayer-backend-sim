package matchmaking

import (
	"time"
)
type QueueStatus string

const (
	QueueStatusQueued    QueueStatus = "QUEUED"
	QueueStatusMatched   QueueStatus = "MATCHED"
	QueueStatusExpired   QueueStatus = "EXPIRED"
	QueueStatusCancelled QueueStatus = "CANCELLED"
)

type QueueEntry struct {
	ID          string      `json:"id"`
	PlayerID    string      `json:"player_id"`
	SkillRating int         `json:"skill_rating"`
	Region      string      `json:"region"`
	Status      QueueStatus `json:"status"`
	JoinedAt    time.Time   `json:"joined_at"`
	MatchedAt   *time.Time  `json:"matched_at,omitempty"`
	ExpiresAt   time.Time   `json:"expires_at"`
}

