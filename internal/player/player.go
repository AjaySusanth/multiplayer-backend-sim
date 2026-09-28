package player

import (
	"time"
)

// Status represents the operational state of a player.
type Status string

const (
	StatusIdle    Status = "IDLE"
	StatusInQueue Status = "IN_QUEUE"
	StatusInMatch Status = "IN_MATCH"
)

// Player represents the domain entity for a registered player.
type Player struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	SkillRating int       `json:"skill_rating"`
	Region      string    `json:"region"`
	Status      Status    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
