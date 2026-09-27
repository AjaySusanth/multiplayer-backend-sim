# multiplayer-backend-sim — Agent Rules

Read this file first in every session. It points to everything else you
need and states the rules that override any assumption you might
otherwise make from the codebase alone.

## Source of truth, in order
1. `docs/PRD.md` — what the system does and does not do (MVP scope).
2. `docs/ARCHITECTURE.md` — what's actually built and how, right now.
3. `docs/MVP_PLAN.md` — the ordered task checklist; work one unchecked
   item at a time.
4. `docs/DECISIONS.md` — why things are the way they are, chronologically.
5. `.agent/rules/workflow.md` — how to work: plan → approve → one unit →
   explain → wait. Read this before writing any code.
6. `.agent/rules/code-style.md` — Go conventions and tooling decisions.
7. `.agent/rules/testing.md` — what needs a test and when.
8. `.agent/rules/teaching.md` — how to explain things while building.

## The one rule that overrides everything else
Any personal Go-learning log, study-plan history, or prior tutorial
progression that may exist in this repo or come up in conversation
documents *how a skill was learned* — it is never a design spec or a
tooling ceiling. Always default to the production-standard tool or
pattern for the task at hand, per `.agent/rules/workflow.md`'s
"Standards over familiarity" section, regardless of what was used while
learning fundamentals.

## Tech stack (current — expected to grow, this is not a whitelist)
Go stdlib `net/http` (existing player/health endpoints) + Chi (new route
groups), `pgx/v5` + `pgxpool`, `go-redis/v9`, PostgreSQL, Redis Streams,
`golang-migrate`, Testcontainers-go, Prometheus (`promhttp`), Docker
Compose, GitHub Actions.

## Scope discipline
`docs/PRD.md` §11 lists what's explicitly deferred (friend-match, group
rooms, gRPC, tracing, dashboards, K8s/KEDA, LLM assistant, canary
rollout). Never propose work that pulls any of this into the current
task, even as a "small" addition — flag it as a future-phase note
instead.

## Before adding a new dependency
Ask first. State what stdlib/existing-dependency alternative was
considered and why it isn't enough. See `.agent/rules/code-style.md`.

## When something looks wrong or outdated in existing code
Flag it as its own short conversation before proposing a fix as a unit.
Don't silently "improve" code outside the current task's scope — see
`.agent/rules/workflow.md`.
