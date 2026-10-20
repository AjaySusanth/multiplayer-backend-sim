//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/liveops"
	"multiplayer-backend-sim/internal/matchmaking"
)

func TestLiveOps_DynamicMatchmakingAndRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1. Boot PostgreSQL and Redis Testcontainers with all migrations applied
	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	liveopsStore := liveops.NewPostgresLiveOpsStore(harness.PGPool)
	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	publisher := matchmaking.NewRedisQueuePublisher(harness.RedisClient)

	// 2. Seed LiveOps Version 1: strict max_skill_difference = 100
	v1JSON, _ := json.Marshal(liveops.MatchmakingConfigPayload{
		MaxSkillDifference:  100,
		MaxQueueWaitSeconds: 300,
		MatchSize:           2,
	})
	v1ID := uuid.New()
	now := time.Now().UTC()
	err = liveopsStore.CreateVersion(ctx, &liveops.ConfigVersion{
		ID:                v1ID,
		Version:           1,
		ConfigurationJSON: v1JSON,
		Status:            liveops.StatusDraft,
		CreatedBy:         "admin-test",
		CreatedAt:         now,
	}, &liveops.AuditLog{
		ID:           uuid.New(),
		ActorID:      "admin-test",
		Action:       liveops.ActionConfigCreated,
		ResourceType: "LIVEOPS_CONFIG",
		ResourceID:   "1",
		CreatedAt:    now,
	})
	if err != nil {
		t.Fatalf("failed to create v1: %v", err)
	}
	if err := liveopsStore.ActivateVersion(ctx, 1, "admin-test"); err != nil {
		t.Fatalf("failed to activate v1: %v", err)
	}

	// 3. Seed LiveOps Version 2 (Draft): relaxed max_skill_difference = 300
	v2JSON, _ := json.Marshal(liveops.MatchmakingConfigPayload{
		MaxSkillDifference:  300,
		MaxQueueWaitSeconds: 300,
		MatchSize:           2,
	})
	err = liveopsStore.CreateVersion(ctx, &liveops.ConfigVersion{
		ID:                uuid.New(),
		Version:           2,
		ConfigurationJSON: v2JSON,
		Status:            liveops.StatusDraft,
		CreatedBy:         "admin-test",
		CreatedAt:         now,
	}, &liveops.AuditLog{
		ID:           uuid.New(),
		ActorID:      "admin-test",
		Action:       liveops.ActionConfigCreated,
		ResourceType: "LIVEOPS_CONFIG",
		ResourceID:   "2",
		CreatedAt:    now,
	})
	if err != nil {
		t.Fatalf("failed to create v2: %v", err)
	}

	// 4. Start Worker with:
	//   - 50ms TTL cache  → config changes are visible within one cache period
	//   - 50ms staleTimeout → XAutoClaim picks up PEL entries after 50ms idle
	//   - 100ms blockTimeout → XReadGroup returns quickly so the loop cycles
	//     back to XAutoClaim and re-evaluates the PEL with the new config
	configProvider := matchmaking.NewLiveOpsConfigProvider(liveopsStore, logger, 50*time.Millisecond)
	workerName := "liveops_test_worker"
	consumer := matchmaking.NewConsumer(
		harness.RedisClient, queueStore, matchStore, logger, workerName,
	).WithStaleTimeout(50 * time.Millisecond).
		WithBlockTimeout(100 * time.Millisecond).
		WithConfigProvider(configProvider)

	if err := consumer.SetupGroup(ctx); err != nil {
		t.Fatalf("failed to setup consumer group: %v", err)
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	go consumer.Run(workerCtx)

	// 5. Seed Player A (1200) and Player B (1450) -> Skill gap is 250 (exceeds v1's 100 tolerance)
	pA := uuid.New()
	pB := uuid.New()
	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'PlayerA', 'NA', 1200, 'IDLE'),
		       ($2, 'PlayerB', 'NA', 1450, 'IDLE')
	`, pA, pB)
	if err != nil {
		t.Fatalf("failed to seed players A and B: %v", err)
	}

	_, _ = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{PlayerID: pA.String(), SkillRating: 1200, Region: "NA"})
	_, _ = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{PlayerID: pB.String(), SkillRating: 1450, Region: "NA"})

	_ = publisher.Publish(ctx, matchmaking.MatchMakingJob{PlayerID: pA.String(), SkillRating: 1200, Region: "NA"})
	_ = publisher.Publish(ctx, matchmaking.MatchMakingJob{PlayerID: pB.String(), SkillRating: 1450, Region: "NA"})

	// Verify that under Version 1 (tolerance 100), players A and B are NOT matched
	time.Sleep(300 * time.Millisecond)
	var matchCount int
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM match_players WHERE player_id IN ($1, $2)`, pA, pB).Scan(&matchCount)
	if err != nil {
		t.Fatalf("failed to count match_players: %v", err)
	}
	if matchCount != 0 {
		t.Fatalf("expected 0 matches under strict v1 config, found %d", matchCount)
	}

	// 6. ACTIVATE VERSION 2 (tolerance = 300). Gap 250 is now within tolerance!
	if err := liveopsStore.ActivateVersion(ctx, 2, "admin-test"); err != nil {
		t.Fatalf("failed to activate v2: %v", err)
	}

	// Poll until the worker's cache refreshes and XAutoClaim re-evaluates the un-ACKed tickets.
	// With a 50ms staleTimeout + 100ms blockTimeout the loop cycles every ~150ms, so
	// 10 s is more than enough headroom even on a slow CI runner.
	var matchedV2 bool
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM match_players WHERE player_id IN ($1, $2)`, pA, pB).Scan(&matchCount)
		if err == nil && matchCount == 2 {
			matchedV2 = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !matchedV2 {
		t.Fatalf("expected players A and B to match after activating v2 (tolerance 300), but they did not match")
	}

	// 7. ROLLBACK TO VERSION 1 (tolerance reverts to 100)
	if err := liveopsStore.RollbackVersion(ctx, 1, "admin-test"); err != nil {
		t.Fatalf("failed to rollback to v1: %v", err)
	}

	// Wait for TTL (50ms) to elapse so worker cache reflects rollback
	time.Sleep(100 * time.Millisecond)

	// Seed Player C (1200) and Player D (1450) -> Skill gap 250
	pC := uuid.New()
	pD := uuid.New()
	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES ($1, 'PlayerC', 'NA', 1200, 'IDLE'),
		       ($2, 'PlayerD', 'NA', 1450, 'IDLE')
	`, pC, pD)
	if err != nil {
		t.Fatalf("failed to seed players C and D: %v", err)
	}

	_, _ = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{PlayerID: pC.String(), SkillRating: 1200, Region: "NA"})
	_, _ = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{PlayerID: pD.String(), SkillRating: 1450, Region: "NA"})

	_ = publisher.Publish(ctx, matchmaking.MatchMakingJob{PlayerID: pC.String(), SkillRating: 1200, Region: "NA"})
	_ = publisher.Publish(ctx, matchmaking.MatchMakingJob{PlayerID: pD.String(), SkillRating: 1450, Region: "NA"})

	// Verify that under rolled-back Version 1, players C and D are NOT matched
	time.Sleep(300 * time.Millisecond)
	err = harness.PGPool.QueryRow(ctx, `SELECT COUNT(*) FROM match_players WHERE player_id IN ($1, $2)`, pC, pD).Scan(&matchCount)
	if err != nil {
		t.Fatalf("failed to count match_players for C and D: %v", err)
	}
	if matchCount != 0 {
		t.Fatalf("expected 0 matches after rollback to v1, found %d", matchCount)
	}
}
