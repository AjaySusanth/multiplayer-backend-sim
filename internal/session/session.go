package session

import (
	"context"
	"errors"
	"time"
)

var ErrSessionNotFound = errors.New("game session not found")

type SessionStatus string

const (
	SessionStatusStarting   SessionStatus = "STARTING"
	SessionStatusInProgress SessionStatus = "IN_PROGRESS"
	SessionStatusFinished   SessionStatus = "FINISHED"
	SessionStatusAborted    SessionStatus = "ABORTED"
)

type GameSession struct {
	ID string `json:"id"`
	MatchID string `json:"match_id"`
	Status SessionStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

type SessionStore interface {
	Create(ctx context.Context,matchID string) (*GameSession,error)
	GetByID(ctx context.Context,sessionID string) (*GameSession,error)
	UpdateStatus(ctx context.Context,sessionID string, status SessionStatus) error
}




