//go:build integration
package integration

import (
	"context"
	"io"
	"errors"
	"log/slog"
	"testing"
	"time"
	"github.com/google/uuid"
	"multiplayer-backend-sim/internal/matchmaking"
)

func TestMatchMakingPipeline_EndToEnd(t *testing.T) {
	ctx,cancel := context.WithTimeout(context.Background(),90*time.Second)

	defer cancel()

	harness,teardown,err := SetupHarness(ctx)
	if err != nil {
		t.Fatalf("failed to setup test harness: %v", err)
	}
	defer teardown()


	player1ID := uuid.New()
	player2ID := uuid.New()

	_, err = harness.PGPool.Exec(ctx, `
		INSERT INTO players (id, name, region, skill_rating, status)
		VALUES 
			($1, 'GamerAlpha', 'NA', 1200, 'IDLE'),
			($2, 'GamerBeta',  'NA', 1250, 'IDLE')
	`, player1ID, player2ID)
	if err != nil {
		t.Fatalf("failed to seed players: %v", err)
	}
	// 3. Initialize Stores, Publisher, and Consumer
	matchStore := matchmaking.NewPostgresMatchStore(harness.PGPool)
	queueStore := matchmaking.NewPostgresQueueStore(harness.PGPool)
	publisher := matchmaking.NewRedisQueuePublisher(harness.RedisClient)
	// Suppress noisy worker logs in test output
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer := matchmaking.NewConsumer(harness.RedisClient, queueStore, matchStore, discardLogger,"matchmaking_worker_1")


	if err := consumer.SetupGroup(ctx); err != nil {
		t.Fatalf("failed to setup consumer group: %v", err)
	}
	// 4. Create queue ticket records in Postgres (simulating what the HTTP handler does)
	entry1, err := queueStore.Create(ctx, matchmaking.CreateQueueEntryInput{
		PlayerID:    player1ID.String(),
		SkillRating: 1200,
		Region:      "NA"})
	if err != nil {
		t.Fatalf("failed to enqueue player 1 in postgres: %v", err)
	}
	entry2, err := queueStore.Create(ctx,matchmaking.CreateQueueEntryInput{
		PlayerID:    player2ID.String(),
		SkillRating: 1250,
		Region:      "NA"})
	if err != nil {
		t.Fatalf("failed to enqueue player 2 in postgres: %v", err)
	}

	err = publisher.Publish(ctx,matchmaking.MatchMakingJob{
		PlayerID: player1ID.String(),
		SkillRating: 1200,
		Region: "NA",
	})

	if err != nil {
		t.Fatalf("failed to publish player 1 job: %v", err)
	}
	err = publisher.Publish(ctx, matchmaking.MatchMakingJob{
		PlayerID:    player2ID.String(),
		SkillRating: 1250,
		Region:      "NA",
	})
	if err != nil {
		t.Fatalf("failed to publish player 2 job: %v", err)
	}

	consumerCtx,cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	go consumer.Run(consumerCtx)
	
	var createdMatchID string
	var matchFound bool
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		row := harness.PGPool.QueryRow(ctx,`
			SELECT match_id from match_players
			WHERE player_id in ($1,$2)
			GROUP BY match_id
			HAVING COUNT(DISTINCT player_id) = 2
		`,player1ID,player2ID)

		if err := row.Scan(&createdMatchID); err == nil {
			matchFound = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !matchFound {
		t.Fatalf("timed out waiting for worker to pair players into a match in postgres")
	}

	m, err := matchStore.GetByID(ctx, createdMatchID)
	if err != nil {
		t.Fatalf("failed to get match by id: %v", err)
	}
	if m.Region != "NA" {
		t.Errorf("expected match region 'NA', got '%s'", m.Region)
	}
	if len(m.Players) != 2 {
		t.Errorf("expected 2 players in match, got %d", len(m.Players))
	}

	_,err = queueStore.GetActiveByPlayerID(ctx, player1ID.String())
	if err == nil {
		t.Errorf("expected player 1 to have no QUEUED entry, but GetActiveByPlayerID returned one")
	} else if !errors.Is(err, matchmaking.ErrQueueEntryNotFound) {
		t.Errorf("unexpected error checking player 1 queue status: %v", err)
	}
	_,err = queueStore.GetActiveByPlayerID(ctx, player2ID.String())
	if err == nil {
		t.Errorf("expected player 2 to have no QUEUED entry, but GetActiveByPlayerID returned one")
	} else if !errors.Is(err, matchmaking.ErrQueueEntryNotFound) {
		t.Errorf("unexpected error checking player 2 queue status: %v", err)
	}
	// 10. Directly query queue_entries to verify rows transitioned to MATCHED (not deleted)
	var status1, status2 string
	err = harness.PGPool.QueryRow(ctx, `
		SELECT status FROM queue_entries WHERE id = $1
	`, entry1.ID).Scan(&status1)
	if err != nil {
		t.Fatalf("failed to query player 1 queue entry: %v", err)
	}
	if status1 != string(matchmaking.QueueStatusMatched) {
		t.Errorf("expected player 1 queue status MATCHED, got %s", status1)
	}
	err = harness.PGPool.QueryRow(ctx, `
		SELECT status FROM queue_entries WHERE id = $1
	`, entry2.ID).Scan(&status2)
	if err != nil {
		t.Fatalf("failed to query player 2 queue entry: %v", err)
	}
	if status2 != string(matchmaking.QueueStatusMatched) {
		t.Errorf("expected player 2 queue status MATCHED, got %s", status2)
	}
}