//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/matchmaking"
)

// TestMatchmaking_TwoWorkersPartitionAndLatency observes and measures how two concurrent
// workers consuming from the same Redis stream handle two compatible players.
// It reports whether the match is formed immediately via fresh XReadGroup batches or
// deferred until XAutoClaim stale-message reclamation.
func TestMatchmaking_TwoWorkersPartitionAndLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	harness, teardown, err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()

	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	publisher := matchmaking.NewRedisQueuePublisher(harness.RedisClient)

	// Set a distinct 1000ms staleTimeout. If workers split the batch and cannot match
	// immediately, they will be forced to wait at least 1000ms for XAutoClaim to reclaim.
	staleTimeout := 1000 * time.Millisecond
	blockTimeout := 50 * time.Millisecond

	worker1 := matchmaking.NewConsumer(
		harness.RedisClient, queueStore, matchStore, discardLogger, "worker-alpha",
	).WithStaleTimeout(staleTimeout).WithBlockTimeout(blockTimeout)

	worker2 := matchmaking.NewConsumer(
		harness.RedisClient, queueStore, matchStore, discardLogger, "worker-beta",
	).WithStaleTimeout(staleTimeout).WithBlockTimeout(blockTimeout)

	if err := worker1.SetupGroup(ctx); err != nil {
		t.Fatalf("failed to setup consumer group: %v", err)
	}

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	go worker1.Run(workerCtx)
	go worker2.Run(workerCtx)

	// Give workers a brief moment to enter their blocking read loops
	time.Sleep(100 * time.Millisecond)

	// 1. Seed two compatible players in PostgreSQL
	p1ID := uuid.New()
	p2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES 
			($1, 'ConcurrentPlayer1', 'NA', 1200, 'IDLE'),
			($2, 'ConcurrentPlayer2', 'NA', 1250, 'IDLE')
	`, p1ID, p2ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}

	_, err = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    p1ID.String(),
		SkillRating: 1200,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 1: %v", err)
	}

	_, err = queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    p2ID.String(),
		SkillRating: 1250,
		Region:      "NA",
		TTL:         5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to enqueue player 2: %v", err)
	}

	// 2. Publish both jobs concurrently to maximize the likelihood of Redis splitting
	// the jobs across worker-alpha and worker-beta
	publishStart := time.Now()
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = publisher.Publish(ctx, matchmaking.MatchMakingJob{
			PlayerID:    p1ID.String(),
			SkillRating: 1200,
			Region:      "NA",
		})
	}()

	go func() {
		defer wg.Done()
		_ = publisher.Publish(ctx, matchmaking.MatchMakingJob{
			PlayerID:    p2ID.String(),
			SkillRating: 1250,
			Region:      "NA",
		})
	}()

	wg.Wait()

	// 3. Poll PostgreSQL until a match containing both players is created
	var matchID string
	var matchFound bool
	var matchLatency time.Duration
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		row := harness.PGPool.QueryRow(ctx, `
			SELECT match_id 
			FROM match_players 
			WHERE player_id IN ($1, $2)
			GROUP BY match_id 
			HAVING COUNT(DISTINCT player_id) = 2
		`, p1ID, p2ID)

		if err := row.Scan(&matchID); err == nil {
			matchLatency = time.Since(publishStart)
			matchFound = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !matchFound {
		t.Fatalf("timed out waiting for match to form between player 1 and player 2")
	}

	// 4. Report empirical findings
	matchedImmediatelyWithoutReclaim := matchLatency < staleTimeout

	t.Logf("\n================= TWO WORKERS CONCURRENCY REPORT =================")
	t.Logf("Match ID: %s", matchID)
	t.Logf("Stale message timeout: %v", staleTimeout)
	t.Logf("Measured match latency: %v", matchLatency)
	t.Logf("Matched immediately on fresh read: %v", matchedImmediatelyWithoutReclaim)
	if matchedImmediatelyWithoutReclaim {
		t.Logf("Result: One worker successfully leased both stream messages in a single batch.")
	} else {
		t.Logf("Result: Stream messages were split across worker-alpha and worker-beta. Players had to wait for the XAutoClaim reclaim sweep (~%v) before being matched.", staleTimeout)
	}
	t.Logf("==================================================================\n")
}
