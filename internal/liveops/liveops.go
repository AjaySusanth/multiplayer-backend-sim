package liveops

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Configuration status enum
const (
	StatusDraft    = "DRAFT"
	StatusActive   = "ACTIVE"
	StatusArchived = "ARCHIVED"
)

// Audit actions
const (
	ActionConfigCreated    = "CONFIG_CREATED"
	ActionConfigActivated  = "CONFIG_ACTIVATED"
	ActionConfigRolledBack = "CONFIG_ROLLED_BACK"
)

// MatchmakingConfigPayload represents the strongly-typed schema for matchmaking settings.
type MatchmakingConfigPayload struct {
	MaxSkillDifference   int `json:"max_skill_difference"`
	MaxQueueWaitSeconds  int `json:"max_queue_wait_seconds"`
	MatchSize            int `json:"match_size"`
}

// Validate ensures all rules in the configuration payload make domain sense.
func (p *MatchmakingConfigPayload) Validate() error {
	if p.MaxSkillDifference <= 0 {
		return errors.New("max_skill_difference must be greater than 0")
	}
	if p.MaxQueueWaitSeconds <= 0 {
		return errors.New("max_queue_wait_seconds must be greater than 0")
	}
	if p.MatchSize < 2 {
		return errors.New("match_size must be at least 2")
	}
	return nil
}

// ConfigVersion represents a versioned configuration snapshot in the database.
type ConfigVersion struct {
	ID                uuid.UUID       `json:"id"`
	Version           int             `json:"version"`
	ConfigurationJSON json.RawMessage `json:"configuration_json"`
	Status            string          `json:"status"`
	CreatedBy         string          `json:"created_by"`
	CreatedAt         time.Time       `json:"created_at"`
	ActivatedAt       *time.Time      `json:"activated_at,omitempty"`
}

// AuditLog represents an immutable record of an administrative action.
type AuditLog struct {
	ID           uuid.UUID       `json:"id"`
	ActorID      string          `json:"actor_id"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	MetadataJSON json.RawMessage `json:"metadata_json,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

// Domain errors
var (
	ErrVersionNotFound   = errors.New("configuration version not found")
	ErrVersionExists     = errors.New("configuration version already exists")
	ErrNoActiveConfig    = errors.New("no active configuration found")
	ErrAlreadyActive     = errors.New("configuration is already active")
	ErrInvalidConfigJSON = errors.New("invalid configuration JSON payload")
)

// LiveOpsStore defines the repository interface for LiveOps and Audit operations.
type LiveOpsStore interface {
	// CreateVersion saves a new configuration draft and emits an audit log atomically.
	CreateVersion(ctx context.Context, config *ConfigVersion, audit *AuditLog) error

	// ActivateVersion archives any currently active configuration, marks the specified version ACTIVE,
	// and records an audit log atomically in one transaction.
	ActivateVersion(ctx context.Context, version int, actorID string) error

	// RollbackVersion transitions back to an older version (marking it ACTIVE, archiving current),
	// and records an audit log specifying it was a rollback.
	RollbackVersion(ctx context.Context, targetVersion int, actorID string) error

	// GetActive returns the current active configuration.
	GetActive(ctx context.Context) (*ConfigVersion, error)

	// ListConfigurations returns all configurations sorted by version descending.
	ListConfigurations(ctx context.Context) ([]ConfigVersion, error)

	// ListAuditLogs returns recent audit logs ordered by creation time descending up to limit.
	ListAuditLogs(ctx context.Context, limit int) ([]AuditLog, error)
}