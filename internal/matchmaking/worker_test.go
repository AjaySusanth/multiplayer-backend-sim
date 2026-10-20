package matchmaking

import (
	"testing"

	"multiplayer-backend-sim/internal/liveops"
)

func TestProcessBatch(t *testing.T) {
	t.Run("matches players within dynamic MaxSkillDifference", func(t *testing.T) {
		cfg := liveops.MatchmakingConfigPayload{
			MaxSkillDifference: 100,
			MatchSize:          2,
		}
		jobs := []MatchMakingJob{
			{PlayerID: "p1", Region: "NA", SkillRating: 1200},
			{PlayerID: "p2", Region: "NA", SkillRating: 1280}, // Diff = 80 (<= 100) -> Match!
		}

		matches := ProcessBatch(jobs, cfg)
		if len(matches) != 1 {
			t.Fatalf("expected 1 match, got %d", len(matches))
		}
		if matches[0][0].PlayerID != "p1" || matches[0][1].PlayerID != "p2" {
			t.Errorf("unexpected match pair: %+v", matches[0])
		}
	})

	t.Run("rejects players exceeding dynamic MaxSkillDifference", func(t *testing.T) {
		cfg := liveops.MatchmakingConfigPayload{
			MaxSkillDifference: 50,
			MatchSize:          2,
		}
		jobs := []MatchMakingJob{
			{PlayerID: "p1", Region: "NA", SkillRating: 1200},
			{PlayerID: "p2", Region: "NA", SkillRating: 1280}, // Diff = 80 (> 50) -> No match
		}

		matches := ProcessBatch(jobs, cfg)
		if len(matches) != 0 {
			t.Fatalf("expected 0 matches, got %d", len(matches))
		}
	})

	t.Run("enforces regional isolation", func(t *testing.T) {
		cfg := liveops.MatchmakingConfigPayload{
			MaxSkillDifference: 500,
			MatchSize:          2,
		}
		jobs := []MatchMakingJob{
			{PlayerID: "p1", Region: "NA", SkillRating: 1200},
			{PlayerID: "p2", Region: "EU", SkillRating: 1200}, // Same skill, different region
		}

		matches := ProcessBatch(jobs, cfg)
		if len(matches) != 0 {
			t.Fatalf("expected 0 matches across different regions, got %d", len(matches))
		}
	})

	t.Run("respects dynamic MatchSize = 4", func(t *testing.T) {
		cfg := liveops.MatchmakingConfigPayload{
			MaxSkillDifference: 200,
			MatchSize:          4,
		}
		jobs := []MatchMakingJob{
			{PlayerID: "p1", Region: "NA", SkillRating: 1000},
			{PlayerID: "p2", Region: "NA", SkillRating: 1050},
			{PlayerID: "p3", Region: "NA", SkillRating: 1100},
		}

		// Only 3 players present, MatchSize is 4 -> No match yet
		matches := ProcessBatch(jobs, cfg)
		if len(matches) != 0 {
			t.Fatalf("expected 0 matches for partial group, got %d", len(matches))
		}

		// 4th player arrives
		jobs = append(jobs, MatchMakingJob{PlayerID: "p4", Region: "NA", SkillRating: 1080})
		matches = ProcessBatch(jobs, cfg)
		if len(matches) != 1 {
			t.Fatalf("expected 1 match when 4th player joined, got %d", len(matches))
		}
		if len(matches[0]) != 4 {
			t.Errorf("expected match size 4, got %d", len(matches[0]))
		}
	})
}