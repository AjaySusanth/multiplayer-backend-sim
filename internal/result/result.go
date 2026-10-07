package result

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// MatchResult represents the outcome of a match for a specific player.
type MatchResult struct {
	ID             uuid.UUID `json:"id"`
	MatchID        uuid.UUID `json:"match_id"`
	PlayerID       uuid.UUID `json:"player_id"`
	Score          int       `json:"score"`
	Result         string    `json:"result"` // e.g., "WIN", "LOSS", "DRAW"
	SubmittedAt    time.Time `json:"submitted_at"`
	IdempotencyKey *string   `json:"idempotency_key,omitempty"`
}

// IdempotencyRecord stores a cached HTTP response for a specific operation.
type IdempotencyRecord struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Operation      string          `json:"operation"`
	ResponseJSON   json.RawMessage `json:"response_json"`
	CreatedAt      time.Time       `json:"created_at"`
}

// ResultStore defines the interface for saving match results and checking idempotency.
type ResultStore interface {
	// SaveResultIdempotent executes a check-and-store for the idempotency key,
	// and if the key doesn't exist, saves the MatchResult in the same transaction.
	// Returns the cached IdempotencyRecord if the request is a duplicate.
	SaveResultIdempotent(ctx context.Context, result *MatchResult, operation string, responseJSON json.RawMessage) (*IdempotencyRecord, error)
}