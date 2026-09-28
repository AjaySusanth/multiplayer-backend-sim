package player

import (
	"context"
	"errors"
)

// Domain sentinel errors for player store operations.
var (
	ErrPlayerNotFound      = errors.New("player not found")
	ErrDuplicatePlayerName = errors.New("player with this name already exists")
)

// CreatePlayerInput holds the payload required to register a new player.
type CreatePlayerInput struct {
	Name        string `json:"name"`
	SkillRating int    `json:"skill_rating"`
	Region      string `json:"region"`
}

// PlayerStore defines the contract for player persistence operations.
type PlayerStore interface {
	Create(ctx context.Context, input CreatePlayerInput) (*Player, error)
	GetByID(ctx context.Context, id string) (*Player, error)
	Delete(ctx context.Context, id string) error
}
