package session

import (
	"math"
	"testing"
)

func TestTagGame_Initialization(t *testing.T) {
	p1 := "player-bravo"
	p2 := "player-alpha"

	game := NewTagGame(p1, p2)

	// 1. Initial "it" must be lexicographically first player ID ("player-alpha")
	if game.CurrentItID != "player-alpha" {
		t.Fatalf("expected initial it to be player-alpha, got %s", game.CurrentItID)
	}

	stateAlpha := game.Players["player-alpha"]
	stateBravo := game.Players["player-bravo"]

	if !stateAlpha.IsIt {
		t.Errorf("expected player-alpha IsIt to be true")
	}
	if stateBravo.IsIt {
		t.Errorf("expected player-bravo IsIt to be false")
	}

	// 2. Fixed start positions on opposite sides
	if stateBravo.Position.X != 100 || stateBravo.Position.Y != 300 {
		t.Errorf("expected p1 start at (100, 300), got (%f, %f)", stateBravo.Position.X, stateBravo.Position.Y)
	}
	if stateAlpha.Position.X != 700 || stateAlpha.Position.Y != 300 {
		t.Errorf("expected p2 start at (700, 300), got (%f, %f)", stateAlpha.Position.X, stateAlpha.Position.Y)
	}

	// 3. Round tick count
	if game.TicksRemaining != TotalRoundTicks {
		t.Errorf("expected ticks remaining %d, got %d", TotalRoundTicks, game.TicksRemaining)
	}
}

func TestTagGame_MovementClampingAndNormalization(t *testing.T) {
	game := NewTagGame("p1", "p2")

	// P1 at (100, 300). Move right with direction (1.0, 0.0)
	game.ApplyDirection("p1", Vector{X: 1.0, Y: 0.0})
	game.Tick()

	// Speed is 100 units/s -> 10 units/tick at 10Hz
	p1Pos := game.Players["p1"].Position
	if p1Pos.X != 110 || p1Pos.Y != 300 {
		t.Fatalf("expected P1 at (110, 300), got (%f, %f)", p1Pos.X, p1Pos.Y)
	}

	// Test diagonal normalization: client sends (10.0, 10.0) trying to run faster
	game.ApplyDirection("p1", Vector{X: 10.0, Y: 10.0})
	game.Tick()

	// Normalized direction has magnitude 1.0, so movement is 10.0 units total
	p1PosAfter := game.Players["p1"].Position
	dx := p1PosAfter.X - p1Pos.X
	dy := p1PosAfter.Y - p1Pos.Y
	distanceMoved := math.Sqrt(dx*dx + dy*dy)

	if math.Abs(distanceMoved-10.0) > 0.001 {
		t.Fatalf("expected normalized movement of 10.0 units, got %f", distanceMoved)
	}

	// Test field boundary clamping: move past FieldWidth (800)
	game.Players["p1"].Position = Vector{X: 795, Y: 595}
	game.ApplyDirection("p1", Vector{X: 1.0, Y: 1.0})
	game.Tick()

	clampedPos := game.Players["p1"].Position
	if clampedPos.X > FieldWidth || clampedPos.Y > FieldHeight {
		t.Fatalf("position exceeded bounds: (%f, %f)", clampedPos.X, clampedPos.Y)
	}

	// Move past (0, 0)
	game.Players["p1"].Position = Vector{X: 5, Y: 5}
	game.ApplyDirection("p1", Vector{X: -1.0, Y: -1.0})
	game.Tick()

	clampedPosZero := game.Players["p1"].Position
	if clampedPosZero.X < 0 || clampedPosZero.Y < 0 {
		t.Fatalf("position fell below zero: (%f, %f)", clampedPosZero.X, clampedPosZero.Y)
	}
}

func TestTagGame_TagCollisionAndCooldown(t *testing.T) {
	// p1 < p2, so p1 is initial "it"
	game := NewTagGame("p1", "p2")

	// Place them within tag radius (dist = 20 <= 25)
	game.Players["p1"].Position = Vector{X: 200, Y: 200}
	game.Players["p2"].Position = Vector{X: 220, Y: 200}

	if game.CurrentItID != "p1" {
		t.Fatalf("expected p1 to be 'it'")
	}

	// Step 1: Tag should trigger
	game.Tick()

	if game.CurrentItID != "p2" {
		t.Fatalf("expected tag to transfer 'it' to p2, but it is %s", game.CurrentItID)
	}
	if game.PreviousItID != "p1" {
		t.Fatalf("expected previous it to be p1, got %s", game.PreviousItID)
	}
	if game.CooldownTicks != TagCooldownTicks {
		t.Fatalf("expected cooldown ticks %d, got %d", TagCooldownTicks, game.CooldownTicks)
	}

	// Step 2: During cooldown (19 ticks), tag-back must NOT trigger even if colliding
	for i := 0; i < TagCooldownTicks-1; i++ {
		game.Tick()
		if game.CurrentItID != "p2" {
			t.Fatalf("tag back triggered during cooldown at tick %d", i)
		}
	}

	// Step 3: On tick 20, cooldown reaches 0 and p2 tags p1 back!
	game.Tick()
	if game.CurrentItID != "p1" {
		t.Fatalf("expected p2 to tag p1 back after cooldown expired, but it is %s", game.CurrentItID)
	}
	if game.CooldownTicks != TagCooldownTicks {
		t.Fatalf("expected cooldown to reset to %d, got %d", TagCooldownTicks, game.CooldownTicks)
	}
}

func TestTagGame_ScoringAndTieBreaker(t *testing.T) {
	t.Run("higher score wins", func(t *testing.T) {
		game := NewTagGame("p1", "p2")
		// Keep them far apart so no tags happen
		game.Players["p1"].Position = Vector{X: 100, Y: 100}
		game.Players["p2"].Position = Vector{X: 700, Y: 500}

		// Initial "it" is p1. So p2 accrues score.
		for i := 0; i < TotalRoundTicks; i++ {
			finished := game.Tick()
			if i == TotalRoundTicks-1 && !finished {
				t.Fatalf("expected game to finish on tick %d", i+1)
			}
		}

		if game.Outcome == nil {
			t.Fatalf("expected outcome to be set")
		}
		if game.Outcome.WinnerID != "p2" {
			t.Fatalf("expected p2 to win with higher score, got %s", game.Outcome.WinnerID)
		}
		if game.Outcome.Scores["p2"] != TotalRoundTicks {
			t.Fatalf("expected p2 score %d, got %d", TotalRoundTicks, game.Outcome.Scores["p2"])
		}
		if game.Outcome.Scores["p1"] != 0 {
			t.Fatalf("expected p1 score 0, got %d", game.Outcome.Scores["p1"])
		}
	})

	t.Run("tie-breaker awards win to player not 'it' at round end", func(t *testing.T) {
		game := NewTagGame("p1", "p2")

		// Artificially give both players equal scores
		game.Players["p1"].Score = 150
		game.Players["p2"].Score = 150

		// p1 is currently "it", so p2 is not "it"
		game.CurrentItID = "p1"
		game.Players["p1"].IsIt = true
		game.Players["p2"].IsIt = false

		game.TicksRemaining = 1
		game.Tick() // finishes round

		if game.Outcome == nil {
			t.Fatalf("expected outcome to be set")
		}
		// On tie, player NOT "it" (p2) wins!
		if game.Outcome.WinnerID != "p2" {
			t.Fatalf("expected p2 to win tie-breaker, got %s", game.Outcome.WinnerID)
		}
		if game.Outcome.LoserID != "p1" {
			t.Fatalf("expected p1 to be loser, got %s", game.Outcome.LoserID)
		}
	})
}

func TestTagGame_Forfeit(t *testing.T) {
	game := NewTagGame("player-1", "player-2")

	outcome := game.Forfeit("player-1")

	if outcome.WinnerID != "player-2" {
		t.Fatalf("expected player-2 to win forfeit, got %s", outcome.WinnerID)
	}
	if outcome.LoserID != "player-1" {
		t.Fatalf("expected player-1 to lose forfeit, got %s", outcome.LoserID)
	}
	if outcome.Reason != "forfeit" {
		t.Fatalf("expected reason 'forfeit', got %s", outcome.Reason)
	}
	if !game.IsFinished {
		t.Fatalf("expected game to be marked finished")
	}
}
