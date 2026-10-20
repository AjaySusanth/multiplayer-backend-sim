package matchmaking

import (
	"math"

	"multiplayer-backend-sim/internal/liveops"
)

// ProcessBatch takes a raw list of pending matchmaking jobs and returns valid, grouped matches
// adhering to the MaxSkillDifference and MatchSize specified in the LiveOps configuration.
func ProcessBatch(jobs []MatchMakingJob, cfg liveops.MatchmakingConfigPayload) [][]MatchMakingJob {
	if cfg.MatchSize < 2 {
		cfg.MatchSize = 2
	}
	if cfg.MaxSkillDifference <= 0 {
		cfg.MaxSkillDifference = 500
	}

	var successfulMatches [][]MatchMakingJob

	// 1. Group jobs by Region (players can only match within their own region)
	groupedByRegion := make(map[string][]MatchMakingJob)
	for _, job := range jobs {
		groupedByRegion[job.Region] = append(groupedByRegion[job.Region], job)
	}

	// 2. Form groups of size cfg.MatchSize within each region group
	for _, regionalJobs := range groupedByRegion {
		matchedIndexes := make(map[int]bool)

		for i := 0; i < len(regionalJobs); i++ {
			if matchedIndexes[i] {
				continue
			}

			seedPlayer := regionalJobs[i]
			candidateGroup := []MatchMakingJob{seedPlayer}
			candidateIndexes := []int{i}

			// Look forward in the queue for compatible partners
			for j := i + 1; j < len(regionalJobs); j++ {
				if matchedIndexes[j] {
					continue
				}

				candidate := regionalJobs[j]
				skillDiff := math.Abs(float64(seedPlayer.SkillRating - candidate.SkillRating))
				if skillDiff <= float64(cfg.MaxSkillDifference) {
					candidateGroup = append(candidateGroup, candidate)
					candidateIndexes = append(candidateIndexes, j)

					if len(candidateGroup) == cfg.MatchSize {
						break
					}
				}
			}

			// If a full group was formed, commit the match
			if len(candidateGroup) == cfg.MatchSize {
				successfulMatches = append(successfulMatches, candidateGroup)
				for _, idx := range candidateIndexes {
					matchedIndexes[idx] = true
				}
			}
		}
	}

	return successfulMatches
}