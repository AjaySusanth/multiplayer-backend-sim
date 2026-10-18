package liveops

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// FakeLiveOpsStore provides a thread-safe in-memory implementation of LiveOpsStore for testing.
type FakeLiveOpsStore struct {
	mu            sync.RWMutex
	Configs       map[int]*ConfigVersion
	AuditLogs     []AuditLog
	SimulatedErr  error
}

func NewFakeLiveOpsStore() *FakeLiveOpsStore {
	return &FakeLiveOpsStore{
		Configs:   make(map[int]*ConfigVersion),
		AuditLogs: make([]AuditLog, 0),
	}
}

func (f *FakeLiveOpsStore) CreateVersion(ctx context.Context, config *ConfigVersion, audit *AuditLog) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.SimulatedErr != nil {
		return f.SimulatedErr
	}

	if _, exists := f.Configs[config.Version]; exists {
		return ErrVersionExists
	}

	// Copy to prevent external mutation
	cfgCopy := *config
	f.Configs[config.Version] = &cfgCopy

	auditCopy := *audit
	f.AuditLogs = append(f.AuditLogs, auditCopy)

	return nil
}

func (f *FakeLiveOpsStore) ActivateVersion(ctx context.Context, version int, actorID string) error {
	return f.transitionStatus(version, actorID, ActionConfigActivated)
}

func (f *FakeLiveOpsStore) RollbackVersion(ctx context.Context, targetVersion int, actorID string) error {
	return f.transitionStatus(targetVersion, actorID, ActionConfigRolledBack)
}

func (f *FakeLiveOpsStore) transitionStatus(targetVersion int, actorID, action string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.SimulatedErr != nil {
		return f.SimulatedErr
	}

	targetCfg, exists := f.Configs[targetVersion]
	if !exists {
		return ErrVersionNotFound
	}

	if targetCfg.Status == StatusActive {
		return ErrAlreadyActive
	}

	now := time.Now().UTC()

	// 1. Archive current active configuration
	for _, cfg := range f.Configs {
		if cfg.Status == StatusActive {
			cfg.Status = StatusArchived
		}
	}

	// 2. Set target configuration to active
	targetCfg.Status = StatusActive
	targetCfg.ActivatedAt = &now

	// 3. Append audit log
	metadata, _ := json.Marshal(map[string]interface{}{
		"target_version": targetVersion,
	})

	f.AuditLogs = append(f.AuditLogs, AuditLog{
		ID:           uuid.New(),
		ActorID:      actorID,
		Action:       action,
		ResourceType: "LIVEOPS_CONFIG",
		ResourceID:   fmt.Sprintf("%d", targetVersion),
		MetadataJSON: metadata,
		CreatedAt:    now,
	})

	return nil
}

func (f *FakeLiveOpsStore) GetActive(ctx context.Context) (*ConfigVersion, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.SimulatedErr != nil {
		return nil, f.SimulatedErr
	}

	for _, cfg := range f.Configs {
		if cfg.Status == StatusActive {
			cfgCopy := *cfg
			return &cfgCopy, nil
		}
	}

	return nil, ErrNoActiveConfig
}

func (f *FakeLiveOpsStore) ListConfigurations(ctx context.Context) ([]ConfigVersion, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.SimulatedErr != nil {
		return nil, f.SimulatedErr
	}

	configs := make([]ConfigVersion, 0, len(f.Configs))
	for _, cfg := range f.Configs {
		configs = append(configs, *cfg)
	}

	// Sort version descending
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].Version > configs[j].Version
	})

	return configs, nil
}

func (f *FakeLiveOpsStore) ListAuditLogs(ctx context.Context, limit int) ([]AuditLog, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.SimulatedErr != nil {
		return nil, f.SimulatedErr
	}

	// Make a shallow copy and sort newest first
	logs := make([]AuditLog, len(f.AuditLogs))
	copy(logs, f.AuditLogs)

	sort.Slice(logs, func(i, j int) bool {
		return logs[i].CreatedAt.After(logs[j].CreatedAt)
	})

	if limit > 0 && limit < len(logs) {
		logs = logs[:limit]
	}

	return logs, nil
}