package session

import (
	"math"
)

const (
	FieldWidth          = 800.0
	FieldHeight         = 600.0
	MaxSpeedUnitsPerSec = 100.0
	TickRateHz          = 10
	MaxDistancePerTick  = MaxSpeedUnitsPerSec / float64(TickRateHz) // 10.0 units/tick
	TagRadius           = 25.0
	TagCooldownTicks    = 20  // 2 seconds at 10Hz
	TotalRoundTicks     = 300 // 30 seconds at 10Hz
	LobbyTimeoutTicks   = 150 // 15 seconds at 10Hz
)

// Vector represents a 2D float coordinate or direction vector.
type Vector struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// Magnitude returns the Euclidean length of the vector.
func (v Vector) Magnitude() float64 {
	return math.Sqrt(v.X*v.X + v.Y*v.Y)
}

// Normalized returns a unit vector pointing in the same direction, or zero vector if magnitude is 0.
func (v Vector) Normalized() Vector {
	mag := v.Magnitude()
	if mag == 0 {
		return Vector{X: 0, Y: 0}
	}
	return Vector{X: v.X / mag, Y: v.Y / mag}
}

// Distance returns Euclidean distance between two vectors.
func (v Vector) Distance(other Vector) float64 {
	dx := v.X - other.X
	dy := v.Y - other.Y
	return math.Sqrt(dx*dx + dy*dy)
}

// PlayerSimState represents the in-memory simulation state of a player.
type PlayerSimState struct {
	ID        string `json:"id"`
	Position  Vector `json:"position"`
	Direction Vector `json:"direction"`
	IsIt      bool   `json:"is_it"`
	Score     int    `json:"score"` // Ticks spent NOT being "it"
}

// GameOutcome encapsulates the authoritative result of a finished tag match.
type GameOutcome struct {
	WinnerID string         `json:"winner_id"`
	LoserID  string         `json:"loser_id"`
	Scores   map[string]int `json:"scores"`
	Reason   string         `json:"reason"` // "round_timeout" or "forfeit"
}

// TagGame encapsulates the pure domain logic of the 2D server-authoritative tag game.
type TagGame struct {
	P1ID           string                     `json:"p1_id"`
	P2ID           string                     `json:"p2_id"`
	Players        map[string]*PlayerSimState `json:"players"`
	CurrentItID    string                     `json:"current_it_id"`
	PreviousItID   string                     `json:"previous_it_id"`
	CooldownTicks  int                        `json:"cooldown_ticks"`
	TicksRemaining int                        `json:"ticks_remaining"`
	IsFinished     bool                       `json:"is_finished"`
	Outcome        *GameOutcome               `json:"outcome,omitempty"`
}

// NewTagGame initializes a 2-player TagGame with fixed spawn positions and initial "it".
func NewTagGame(p1ID, p2ID string) *TagGame {
	// Fixed opposite start positions
	pos1 := Vector{X: 100, Y: 300}
	pos2 := Vector{X: 700, Y: 300}

	// Initial "it" is lexicographically first player ID
	initialIt := p1ID
	if p2ID < p1ID {
		initialIt = p2ID
	}

	players := make(map[string]*PlayerSimState, 2)
	players[p1ID] = &PlayerSimState{
		ID:        p1ID,
		Position:  pos1,
		Direction: Vector{X: 0, Y: 0},
		IsIt:      p1ID == initialIt,
		Score:     0,
	}
	players[p2ID] = &PlayerSimState{
		ID:        p2ID,
		Position:  pos2,
		Direction: Vector{X: 0, Y: 0},
		IsIt:      p2ID == initialIt,
		Score:     0,
	}

	return &TagGame{
		P1ID:           p1ID,
		P2ID:           p2ID,
		Players:        players,
		CurrentItID:    initialIt,
		PreviousItID:   "",
		CooldownTicks:  0,
		TicksRemaining: TotalRoundTicks,
		IsFinished:     false,
	}
}

// ApplyDirection sets a player's movement direction input, normalizing if magnitude > 1.
func (g *TagGame) ApplyDirection(playerID string, dir Vector) {
	if g.IsFinished {
		return
	}
	p, ok := g.Players[playerID]
	if !ok {
		return
	}

	if dir.Magnitude() > 1.0 {
		p.Direction = dir.Normalized()
	} else {
		p.Direction = dir
	}
}

// Tick executes one simulation step (100ms / 10Hz).
// Returns true when the match finishes on this tick.
func (g *TagGame) Tick() bool {
	if g.IsFinished {
		return true
	}

	// 1. Decrement tag cooldown if active
	if g.CooldownTicks > 0 {
		g.CooldownTicks--
	}

	// 2. Integrate server-authoritative positions and clamp to field boundary
	for _, p := range g.Players {
		dx := p.Direction.X * MaxDistancePerTick
		dy := p.Direction.Y * MaxDistancePerTick

		newX := p.Position.X + dx
		newY := p.Position.Y + dy

		// Clamp to field [0, FieldWidth] and [0, FieldHeight]
		if newX < 0 {
			newX = 0
		} else if newX > FieldWidth {
			newX = FieldWidth
		}

		if newY < 0 {
			newY = 0
		} else if newY > FieldHeight {
			newY = FieldHeight
		}

		p.Position.X = newX
		p.Position.Y = newY
	}

	// 3. Tag collision check (Euclidean distance <= TagRadius)
	p1 := g.Players[g.P1ID]
	p2 := g.Players[g.P2ID]
	dist := p1.Position.Distance(p2.Position)

	if dist <= TagRadius && g.CooldownTicks == 0 {
		// Transfer "it" role to the other player
		oldIt := g.CurrentItID
		var newIt string
		if oldIt == g.P1ID {
			newIt = g.P2ID
		} else {
			newIt = g.P1ID
		}

		g.PreviousItID = oldIt
		g.CurrentItID = newIt
		g.CooldownTicks = TagCooldownTicks // 20 ticks = 2s at 10Hz

		p1.IsIt = (p1.ID == newIt)
		p2.IsIt = (p2.ID == newIt)
	}

	// 4. Score accumulation: +1 tick for the player NOT "it"
	for _, p := range g.Players {
		if !p.IsIt {
			p.Score++
		}
	}

	// 5. Decrement round countdown
	g.TicksRemaining--
	if g.TicksRemaining <= 0 {
		g.resolveTimeoutWinner()
		return true
	}

	return false
}

// resolveTimeoutWinner calculates the final outcome when round ticks reach 0.
func (g *TagGame) resolveTimeoutWinner() {
	g.IsFinished = true

	p1 := g.Players[g.P1ID]
	p2 := g.Players[g.P2ID]

	scores := map[string]int{
		p1.ID: p1.Score,
		p2.ID: p2.Score,
	}

	var winnerID, loserID string
	if p1.Score > p2.Score {
		winnerID, loserID = p1.ID, p2.ID
	} else if p2.Score > p1.Score {
		winnerID, loserID = p2.ID, p1.ID
	} else {
		// Tie-breaker: player who is NOT "it" at the final tick wins.
		if !p1.IsIt {
			winnerID, loserID = p1.ID, p2.ID
		} else {
			winnerID, loserID = p2.ID, p1.ID
		}
	}

	g.Outcome = &GameOutcome{
		WinnerID: winnerID,
		LoserID:  loserID,
		Scores:   scores,
		Reason:   "round_timeout",
	}
}

// Forfeit resolves the match when a player disconnects mid-round.
func (g *TagGame) Forfeit(leaverID string) *GameOutcome {
	if g.IsFinished && g.Outcome != nil {
		return g.Outcome
	}

	g.IsFinished = true

	var winnerID string
	if leaverID == g.P1ID {
		winnerID = g.P2ID
	} else {
		winnerID = g.P1ID
	}

	scores := make(map[string]int, len(g.Players))
	for id, p := range g.Players {
		scores[id] = p.Score
	}

	g.Outcome = &GameOutcome{
		WinnerID: winnerID,
		LoserID:  leaverID,
		Scores:   scores,
		Reason:   "forfeit",
	}
	return g.Outcome
}
