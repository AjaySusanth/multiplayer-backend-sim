package matchmaking

import "math"

// MaxSkillDifference is the maximum allowed Elo gap between two players.
const MaxSkillDifference = 500

// ProcessBatch takes a raw list of pending matchmaking jobs and returns valid, paired matches.
func ProcessBatch(jobs []MatchMakingJob) [][]MatchMakingJob {
	var successfulMatches [][]MatchMakingJob

	// 1. Group jobs by Region (players can only match within their own region)
	groupedByRegion := make(map[string][]MatchMakingJob)
	for _, job := range jobs {
		groupedByRegion[job.Region] = append(groupedByRegion[job.Region], job)
	}

	// 2. Pair players within each region group
	for _, regionalJobs := range groupedByRegion {
		
		// Track which indexes have already been paired so we don't double-match anyone
		matchedIndexes := make(map[int]bool)

		for i := 0; i < len(regionalJobs); i++ {
			if matchedIndexes[i] {
				continue // Player A is already matched
			}

			playerA := regionalJobs[i]

			// Look forward in the queue for a compatible partner
			for j := i + 1; j < len(regionalJobs); j++ {
				if matchedIndexes[j] {
					continue // Player B is already matched
				}

				playerB := regionalJobs[j]

				// Constraint: Are their skill ratings close enough?
				skillDiff := math.Abs(float64(playerA.SkillRating - playerB.SkillRating))
				if skillDiff <= MaxSkillDifference {
					
					// Match found! Pair them up.
					successfulMatches = append(successfulMatches, []MatchMakingJob{playerA, playerB})
					
					// Mark both as matched
					matchedIndexes[i] = true
					matchedIndexes[j] = true
					break // Stop looking for partners for Player A
				}
			}
		}
	}

	return successfulMatches
}