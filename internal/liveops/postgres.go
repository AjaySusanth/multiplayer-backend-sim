package liveops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresLiveOpsStore struct {
	pool *pgxpool.Pool
}

func NewPostgresLiveOpsStore(pool *pgxpool.Pool) *PostgresLiveOpsStore {
	return &PostgresLiveOpsStore{pool: pool}
}

// CreateVersion inserts a new configuration version and an audit log inside one transaction.
func (s *PostgresLiveOpsStore) CreateVersion(ctx context.Context, cfg *ConfigVersion, audit *AuditLog) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Insert configuration version
	_, err = tx.Exec(ctx, `
		INSERT INTO liveops_configurations (id, version, configuration_json, status, created_by, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, cfg.ID, cfg.Version, cfg.ConfigurationJSON, cfg.Status, cfg.CreatedBy, cfg.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation on version
			return ErrVersionExists
		}
		return fmt.Errorf("inserting configuration version: %w", err)
	}

	// 2. Insert audit log
	_, err = tx.Exec(ctx, `
		INSERT INTO audit_logs (id, actor_id, action, resource_type, resource_id, metadata_json, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, audit.ID, audit.ActorID, audit.Action, audit.ResourceType, audit.ResourceID, audit.MetadataJSON, audit.CreatedAt)
	if err != nil {
		return fmt.Errorf("inserting audit log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// ActivateVersion archives any existing ACTIVE config and sets the specified version to ACTIVE.
func (s *PostgresLiveOpsStore) ActivateVersion(ctx context.Context, version int, actorID string) error {
	return s.transitionVersionStatus(ctx, version, actorID, ActionConfigActivated)
}

// RollbackVersion performs the same atomic swap as activation, but stamps the audit action as CONFIG_ROLLED_BACK.
func (s *PostgresLiveOpsStore) RollbackVersion(ctx context.Context, targetVersion int, actorID string) error {
	return s.transitionVersionStatus(ctx, targetVersion, actorID, ActionConfigRolledBack)
}

// transitionVersionStatus handles the atomic swap and audit log creation for activation & rollback.
func (s *PostgresLiveOpsStore) transitionVersionStatus(ctx context.Context, targetVersion int, actorID, action string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Check target version exists and its current status
	var currentStatus string
	err = tx.QueryRow(ctx, `
		SELECT status FROM liveops_configurations WHERE version = $1 FOR UPDATE
	`, targetVersion).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrVersionNotFound
		}
		return fmt.Errorf("checking target version: %w", err)
	}

	if currentStatus == StatusActive {
		return ErrAlreadyActive
	}

	now := time.Now().UTC()

	// 2. Archive any currently active version
	_, err = tx.Exec(ctx, `
		UPDATE liveops_configurations
		SET status = $1
		WHERE status = $2
	`, StatusArchived, StatusActive)
	if err != nil {
		return fmt.Errorf("archiving current active config: %w", err)
	}

	// 3. Mark the target version as ACTIVE
	_, err = tx.Exec(ctx, `
		UPDATE liveops_configurations
		SET status = $1, activated_at = $2
		WHERE version = $3
	`, StatusActive, now, targetVersion)
	if err != nil {
		return fmt.Errorf("activating target version: %w", err)
	}

	// 4. Record audit log
	metadata, _ := json.Marshal(map[string]interface{}{
		"target_version": targetVersion,
		"previous_state": currentStatus,
	})

	_, err = tx.Exec(ctx, `
		INSERT INTO audit_logs (id, actor_id, action, resource_type, resource_id, metadata_json, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, uuid.New(), actorID, action, "LIVEOPS_CONFIG", fmt.Sprintf("%d", targetVersion), metadata, now)
	if err != nil {
		return fmt.Errorf("inserting audit log: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// GetActive retrieves the current active configuration.
func (s *PostgresLiveOpsStore) GetActive(ctx context.Context) (*ConfigVersion, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, version, configuration_json, status, created_by, created_at, activated_at
		FROM liveops_configurations
		WHERE status = $1
	`, StatusActive)

	var cfg ConfigVersion
	err := row.Scan(&cfg.ID, &cfg.Version, &cfg.ConfigurationJSON, &cfg.Status, &cfg.CreatedBy, &cfg.CreatedAt, &cfg.ActivatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoActiveConfig
		}
		return nil, fmt.Errorf("getting active config: %w", err)
	}

	return &cfg, nil
}

// ListConfigurations returns all configurations ordered by version descending.
func (s *PostgresLiveOpsStore) ListConfigurations(ctx context.Context) ([]ConfigVersion, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, version, configuration_json, status, created_by, created_at, activated_at
		FROM liveops_configurations
		ORDER BY version DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("listing configurations: %w", err)
	}
	defer rows.Close()

	var configs []ConfigVersion
	for rows.Next() {
		var cfg ConfigVersion
		if err := rows.Scan(&cfg.ID, &cfg.Version, &cfg.ConfigurationJSON, &cfg.Status, &cfg.CreatedBy, &cfg.CreatedAt, &cfg.ActivatedAt); err != nil {
			return nil, fmt.Errorf("scanning configuration: %w", err)
		}
		configs = append(configs, cfg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating configurations: %w", err)
	}

	if configs == nil {
		return []ConfigVersion{}, nil
	}
	return configs, nil
}

// ListAuditLogs returns recent audit logs ordered by created_at descending.
func (s *PostgresLiveOpsStore) ListAuditLogs(ctx context.Context, limit int) ([]AuditLog, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, actor_id, action, resource_type, resource_id, metadata_json, created_at
		FROM audit_logs
		ORDER BY created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("listing audit logs: %w", err)
	}
	defer rows.Close()

	var logs []AuditLog
	for rows.Next() {
		var l AuditLog
		if err := rows.Scan(&l.ID, &l.ActorID, &l.Action, &l.ResourceType, &l.ResourceID, &l.MetadataJSON, &l.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning audit log: %w", err)
		}
		logs = append(logs, l)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating audit logs: %w", err)
	}

	if logs == nil {
		return []AuditLog{}, nil
	}
	return logs, nil
}