package matchmaking

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/liveops"
)

func TestLiveOpsConfigProvider(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	t.Run("returns default when no active config exists", func(t *testing.T) {
		fakeStore := liveops.NewFakeLiveOpsStore()
		provider := NewLiveOpsConfigProvider(fakeStore, discardLogger, 100*time.Millisecond)

		cfg := provider.GetConfig(ctx)
		if cfg.MaxSkillDifference != 500 || cfg.MatchSize != 2 || cfg.MaxQueueWaitSeconds != 300 {
			t.Fatalf("expected defaults, got %+v", cfg)
		}
	})

	t.Run("returns active configuration from store", func(t *testing.T) {
		fakeStore := liveops.NewFakeLiveOpsStore()
		raw, _ := json.Marshal(liveops.MatchmakingConfigPayload{
			MaxSkillDifference:  250,
			MaxQueueWaitSeconds: 60,
			MatchSize:           4,
		})
		fakeStore.Configs[1] = &liveops.ConfigVersion{
			ID:                uuid.New(),
			Version:           1,
			ConfigurationJSON: raw,
			Status:            liveops.StatusActive,
		}

		provider := NewLiveOpsConfigProvider(fakeStore, discardLogger, 100*time.Millisecond)
		cfg := provider.GetConfig(ctx)

		if cfg.MaxSkillDifference != 250 || cfg.MatchSize != 4 || cfg.MaxQueueWaitSeconds != 60 {
			t.Fatalf("expected active config (250/60/4), got %+v", cfg)
		}
	})

	t.Run("serves from cache within TTL without hitting store", func(t *testing.T) {
		fakeStore := liveops.NewFakeLiveOpsStore()
		rawV1, _ := json.Marshal(liveops.MatchmakingConfigPayload{
			MaxSkillDifference:  100,
			MaxQueueWaitSeconds: 30,
			MatchSize:           2,
		})
		fakeStore.Configs[1] = &liveops.ConfigVersion{
			ID:                uuid.New(),
			Version:           1,
			ConfigurationJSON: rawV1,
			Status:            liveops.StatusActive,
		}

		// 1 second TTL
		provider := NewLiveOpsConfigProvider(fakeStore, discardLogger, 1*time.Second)
		cfg1 := provider.GetConfig(ctx)
		if cfg1.MaxSkillDifference != 100 {
			t.Fatalf("expected 100, got %d", cfg1.MaxSkillDifference)
		}

		// Update store behind the scenes
		rawV2, _ := json.Marshal(liveops.MatchmakingConfigPayload{
			MaxSkillDifference:  400,
			MaxQueueWaitSeconds: 30,
			MatchSize:           2,
		})
		fakeStore.Configs[1].ConfigurationJSON = rawV2

		// Immediate second call should still hit cache (returns 100, not 400)
		cfg2 := provider.GetConfig(ctx)
		if cfg2.MaxSkillDifference != 100 {
			t.Fatalf("expected cached 100 within TTL, got %d", cfg2.MaxSkillDifference)
		}
	})

	t.Run("refreshes when TTL expires", func(t *testing.T) {
		fakeStore := liveops.NewFakeLiveOpsStore()
		rawV1, _ := json.Marshal(liveops.MatchmakingConfigPayload{
			MaxSkillDifference:  100,
			MaxQueueWaitSeconds: 30,
			MatchSize:           2,
		})
		fakeStore.Configs[1] = &liveops.ConfigVersion{
			ID:                uuid.New(),
			Version:           1,
			ConfigurationJSON: rawV1,
			Status:            liveops.StatusActive,
		}

		// Short 20ms TTL
		provider := NewLiveOpsConfigProvider(fakeStore, discardLogger, 20*time.Millisecond)
		cfg1 := provider.GetConfig(ctx)
		if cfg1.MaxSkillDifference != 100 {
			t.Fatalf("expected 100, got %d", cfg1.MaxSkillDifference)
		}

		// Update store
		rawV2, _ := json.Marshal(liveops.MatchmakingConfigPayload{
			MaxSkillDifference:  400,
			MaxQueueWaitSeconds: 30,
			MatchSize:           2,
		})
		fakeStore.Configs[1].ConfigurationJSON = rawV2

		// Wait for TTL to expire
		time.Sleep(30 * time.Millisecond)

		cfg2 := provider.GetConfig(ctx)
		if cfg2.MaxSkillDifference != 400 {
			t.Fatalf("expected refreshed 400 after TTL expiration, got %d", cfg2.MaxSkillDifference)
		}
	})

	t.Run("safely retains cached config when store errors", func(t *testing.T) {
		fakeStore := liveops.NewFakeLiveOpsStore()
		raw, _ := json.Marshal(liveops.MatchmakingConfigPayload{
			MaxSkillDifference:  200,
			MaxQueueWaitSeconds: 40,
			MatchSize:           2,
		})
		fakeStore.Configs[1] = &liveops.ConfigVersion{
			ID:                uuid.New(),
			Version:           1,
			ConfigurationJSON: raw,
			Status:            liveops.StatusActive,
		}

		provider := NewLiveOpsConfigProvider(fakeStore, discardLogger, 10*time.Millisecond)
		cfg1 := provider.GetConfig(ctx)
		if cfg1.MaxSkillDifference != 200 {
			t.Fatalf("expected 200, got %d", cfg1.MaxSkillDifference)
		}

		// Inject simulated database failure
		fakeStore.SimulatedErr = errors.New("postgres connection terminated")
		time.Sleep(15 * time.Millisecond)

		// Must not panic or return zero values; retains previous valid config
		cfg2 := provider.GetConfig(ctx)
		if cfg2.MaxSkillDifference != 200 {
			t.Fatalf("expected retained cached 200 on error, got %d", cfg2.MaxSkillDifference)
		}
	})
}