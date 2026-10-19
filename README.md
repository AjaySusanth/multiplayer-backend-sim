# Multiplayer Backend Simulation

A production-grade, distributed multiplayer game backend simulation built in Go.

This platform simulates the complete backend lifecycle of a multiplayer game: player management, matchmaking with Redis Streams, 10Hz tick-rate real-time game sessions over WebSockets, idempotent match results, relational leaderboards via PostgreSQL window functions, and dynamic LiveOps runtime configurations with an append-only audit trail.

---

## Architecture Overview

```
                      +-------------------+
                      |   Client / Game   |
                      +---------+---------+
                                |
             +------------------+------------------+
             | HTTP (REST)                         | WebSocket (10Hz)
             v                                     v
    +-----------------+                  +-------------------+
    |   API Server    |                  |  Real-Time Engine |
    |   (Chi Router)  |                  | (Tag Simulation)  |
    +---+---------+---+                  +-------------------+
        |         |
        |         +-----------------------+
        v                                 v
+---------------+                +-------------------+
|  PostgreSQL   |                |   Redis Streams   |
| (pgxpool v5)  |                |   (go-redis v9)   |
+---------------+                +---------+---------+
        ^                                  |
        |       +--------------------+     |
        +-------+ Matchmaking Worker <-----+
                | (Consumer Group)   |
                +--------------------+
```

### Core Subsystems

1. **Player Service (`internal/player`)**:
   - Player CRUD with skill ratings (Elo), regions, and statuses.
   - Backed by PostgreSQL with `pgxpool/v5`.
2. **Matchmaking Pipeline (`internal/matchmaking`)**:
   - Ticket ingestion producing events to Redis Streams (`matchmaking_stream`).
   - Standalone background worker (`cmd/worker`) pulling jobs via consumer groups.
   - Batching algorithm applying regional isolation and skill-difference constraints (`MaxSkillDifference = 500`).
   - Atomic multi-table match creation (`matches` and `match_players`) in PostgreSQL.
3. **Worker Fault-Tolerance & Recovery**:
   - Automatic Redis Streams PEL (Pending Entries List) recovery using `XAUTOCLAIM`.
   - Idempotent match creation protecting against duplicate match assignments if workers crash mid-flight.
4. **Real-Time Game Sessions (`internal/session`)**:
   - 10Hz in-memory tag simulation engine over WebSockets (`/ws/session/{session_id}`).
   - Tick broadcast loop and player movement input synchronization.
5. **Idempotent Match Results (`internal/result`)**:
   - Outcome reporting with strict `Idempotency-Key` header enforcement.
   - Atomic check-and-store pattern backed by PostgreSQL unique constraint (`23505`) to safely return cached responses on retries.
6. **Relational Leaderboard (`internal/leaderboard`)**:
   - Score aggregation and ranking pushed directly to PostgreSQL using Common Table Expressions (CTEs) and `RANK() OVER (ORDER BY SUM(score) DESC)`.
7. **LiveOps & Audit Trail (`internal/liveops`)**:
   - Strongly-typed, versioned runtime configuration management (e.g. queue timeouts, Elo tolerances).
   - Single-active configuration enforced by PostgreSQL partial unique index (`WHERE status = 'ACTIVE'`).
   - Atomic version activation and rollback with pessimistic row-level locking (`FOR UPDATE`).
   - Immutable, append-only audit trail recording every administrative mutation.

---

## Quickstart (Docker Compose)

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) & Docker Compose
- [Go 1.24+](https://go.dev/dl/) (optional, for local test execution)

### 1. Launch the Stack
Start all 4 services (PostgreSQL, Redis, API Server, Matchmaking Worker) in detached mode:
```bash
docker compose up -d
```

### 2. Verify Container Health
```bash
docker compose ps
```
All containers should be running, with `multiplayer_postgres` and `multiplayer_redis` marked as `(healthy)`.

### 3. Check Live Logs
```bash
docker compose logs -f api
docker compose logs -f worker
```

### 4. Teardown
```bash
docker compose down
```

---

## Testing

### Unit Tests (Fast, In-Memory)
Unit tests run against thread-safe, in-memory fake stores with zero external dependencies:
```bash
go test -v ./internal/...
```

### Integration Tests (Testcontainers-go)
The integration test suite dynamically provisions real `postgres:16-alpine` and `redis:7-alpine` Docker containers, executes all migrations (`000001` through `000008`), and verifies distributed invariants:
```bash
go test -v -tags=integration ./tests/integration/...
```
- **Test 1:** Concurrent match-result submissions validating PostgreSQL `23505` transaction abort and idempotent cached replay.
- **Test 2:** End-to-end matchmaking ticket publishing, worker consumption, and database state transitions.
- **Test 3:** Simulated worker crash leaving un-ACKed tickets in Redis PEL, followed by autonomous recovery and processing via `XAUTOCLAIM`.

---

## API Walkthrough

All API routes are grouped under `/api/v1` (with operational and WebSocket endpoints at root).

### 1. Register Two Players
```bash
# Player 1 (Alex)
curl -X POST http://localhost:8080/api/v1/players \
  -H "Content-Type: application/json" \
  -d '{"name":"Alex","region":"NA","skill_rating":1200}'

# Player 2 (Jordan)
curl -X POST http://localhost:8080/api/v1/players \
  -H "Content-Type: application/json" \
  -d '{"name":"Jordan","region":"NA","skill_rating":1250}'
```

### 2. Enter Matchmaking Queue
```bash
curl -X POST http://localhost:8080/api/v1/matchmaking/join \
  -H "Content-Type: application/json" \
  -d '{"player_id":"<ALEX_ID>"}'

curl -X POST http://localhost:8080/api/v1/matchmaking/join \
  -H "Content-Type: application/json" \
  -d '{"player_id":"<JORDAN_ID>"}'
```

### 3. Check Matchmaking Status
```bash
curl http://localhost:8080/api/v1/matchmaking/status/<ALEX_ID>
```
*Returns `200 OK` with `"status": "MATCHED"` and the generated `match_id`.*

### 4. Submit Match Outcome (Idempotent)
```bash
curl -X POST http://localhost:8080/api/v1/matches/<MATCH_ID>/result \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-alex-win" \
  -d '{"player_id":"<ALEX_ID>","score":150,"result":"WIN"}'
```
*Returns `201 Created`. Sending the exact same request again returns `200 OK` with the cached response without creating duplicate rows.*

### 5. Query the Leaderboard
```bash
# Global top ranked players
curl http://localhost:8080/api/v1/leaderboards

# Specific player rank
curl http://localhost:8080/api/v1/leaderboards/<ALEX_ID>/rank
```

### 6. LiveOps Configuration & Audit Trail
```bash
# Create version 1 configuration draft
curl -X POST http://localhost:8080/api/v1/liveops/configurations \
  -H "Content-Type: application/json" \
  -d '{"version":1,"actor_id":"admin-1","configuration":{"max_skill_difference":600,"max_queue_wait_seconds":45,"match_size":2}}'

# Atomically activate version 1
curl -X POST http://localhost:8080/api/v1/liveops/configurations/1/activate \
  -H "X-Actor-ID: admin-1"

# View the immutable audit log
curl http://localhost:8080/api/v1/liveops/audit-logs
```

---

## CI / CD
Automated testing is configured in `.github/workflows/ci.yml`:
- Verification of Go modules and dependencies
- Static analysis via `go vet`
- Race detector unit tests (`-race`)
- Testcontainers integration tests against real Docker services
- Standalone compilation of `cmd/server` and `cmd/worker`
- Docker container image build validation
