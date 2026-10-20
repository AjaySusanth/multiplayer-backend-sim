package matchmaking

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"multiplayer-backend-sim/internal/liveops"
)

// DefaultMatchmakingConfig returns the baseline configuration used
// when no active LiveOps version exists or PostgreSQL is unreachable.
func DefaultMatchmakingConfig() liveops.MatchmakingConfigPayload {
	return liveops.MatchmakingConfigPayload{
		MaxSkillDifference:  500,
		MaxQueueWaitSeconds: 300,
		MatchSize:           2,
	}
}

// ConfigProvider abstracts retrieving the active matchmaking configuration.
type ConfigProvider interface {
	GetConfig(ctx context.Context) liveops.MatchmakingConfigPayload
}

// LiveOpsConfigProvider caches the active LiveOps configuration in memory
// with a short TTL to prevent hammering PostgreSQL on every matchmaking loop.
type LiveOpsConfigProvider struct {
	store    liveops.LiveOpsStore
	logger   *slog.Logger
	ttl      time.Duration

	mu       sync.RWMutex
	cached   liveops.MatchmakingConfigPayload
	cachedAt time.Time
}

func NewLiveOpsConfigProvider(store liveops.LiveOpsStore, logger *slog.Logger, ttl time.Duration) *LiveOpsConfigProvider {
	return &LiveOpsConfigProvider{
		store:  store,
		logger: logger,
		ttl:    ttl,
		cached: DefaultMatchmakingConfig(),
	}
}

// GetConfig returns the active matchmaking config from cache, or refreshes from PostgreSQL if the TTL has expired.
func (p *LiveOpsConfigProvider) GetConfig(ctx context.Context) liveops.MatchmakingConfigPayload {
	p.mu.RLock()
	if !p.cachedAt.IsZero() && time.Since(p.cachedAt) < p.ttl {
		cached := p.cached
		p.mu.RUnlock()
		return cached
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check under write lock in case another goroutine refreshed it
	if !p.cachedAt.IsZero() && time.Since(p.cachedAt) < p.ttl {
		return p.cached
	}

	activeVer, err := p.store.GetActive(ctx)
	if err != nil {
		if !errors.Is(err, liveops.ErrNoActiveConfig) {
			p.logger.Warn("failed to fetch active liveops config, falling back to cached/default", "error", err)
		}
		if p.cachedAt.IsZero() {
			p.cached = DefaultMatchmakingConfig()
		}
		// Refresh timestamp so a dead DB doesn't cause lock contention on every tick
		p.cachedAt = time.Now()
		return p.cached
	}

	var payload liveops.MatchmakingConfigPayload
	if err := json.Unmarshal(activeVer.ConfigurationJSON, &payload); err != nil {
		p.logger.Error("malformed liveops configuration_json, falling back to cached/default", "error", err)
		if p.cachedAt.IsZero() {
			p.cached = DefaultMatchmakingConfig()
		}
		p.cachedAt = time.Now()
		return p.cached
	}

	if err := payload.Validate(); err != nil {
		p.logger.Error("active liveops config failed validation, falling back to cached/default", "error", err)
		if p.cachedAt.IsZero() {
			p.cached = DefaultMatchmakingConfig()
		}
		p.cachedAt = time.Now()
		return p.cached
	}

	p.cached = payload
	p.cachedAt = time.Now()
	return p.cached
}