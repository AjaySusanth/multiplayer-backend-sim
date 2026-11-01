# multiplayer-backend-sim — Agent Rules

Read this file first in every session. It points to everything else you
need and states the rules that override any assumption you might
otherwise make from the codebase alone.

## Project status
The MVP and the follow-up hardening pass are complete (see
`docs/MVP_PLAN.md`, now archived). The project is in its **post-MVP
phase**: all new work comes from `docs/ROADMAP.md`, one phase at a time,
in order.

## Source of truth, in order
1. `docs/PRD.md` — what the system does, what is planned, and what is
   still out of scope.
2. `docs/ARCHITECTURE.md` — what is actually built and how, right now,
   including open verification items.
3. `docs/ROADMAP.md` — the ordered post-MVP phases; work one unchecked
   item at a time, and never start a later phase early.
4. `docs/DECISIONS.md` — why things are the way they are,
   chronologically.
5. `docs/MVP_PLAN.md` — archived MVP checklist. Reference only.
6. `.agent/rules/workflow.md` — how to work: plan → approve → one unit →
   explain → wait. Read this before writing any code.
7. `.agent/rules/code-style.md` — Go conventions and tooling decisions.
8. `.agent/rules/testing.md` — what needs a test and when.
9. `.agent/rules/teaching.md` — how to explain things while building.

## The one rule that overrides everything else
Any personal Go-learning log, study-plan history, or prior tutorial
progression that may exist in this repo or come up in conversation
documents *how a skill was learned* — it is never a design spec or a
tooling ceiling. Always default to the production-standard tool or
pattern for the task at hand, per `.agent/rules/workflow.md`'s
"Standards over familiarity" section, regardless of what was used while
learning fundamentals.

## Verify before assuming
Do not infer what the code does from docs, reports, or file names.
Before planning any roadmap item, inspect the actual code and state what
you found, with file paths. If a doc and the code disagree, report the
disagreement; don't silently pick one.

## Report honestly
When reporting status, separate what is **proven by a test** from what is
**assumed or untested**. Don't use phrases like "production-ready" or
"100% complete". State the evidence (which test, which command) and list
known limitations. Performance statements require measured results from
a documented load test.

## Tech stack (current — expected to grow, this is not a whitelist)
Go, Chi (all routes), `pgx/v5` + `pgxpool`, `go-redis/v9`, PostgreSQL 16,
Redis 7 Streams, `golang-migrate`, `gorilla/websocket`, `log/slog`,
Prometheus (`promhttp`), Testcontainers-go, Docker (multi-stage) +
Docker Compose, GitHub Actions.

Planned by the roadmap (not yet adopted — each still needs the usual
approval step): k6 or a Go load generator, Grafana, OpenTelemetry,
Helm, Kubernetes, KEDA, JWT, and possibly gRPC (only if the session
server is extracted).

## Scope discipline
Work comes from the current phase in `docs/ROADMAP.md`. Do not pull in
items from later phases, even as a "small" addition — flag them as
notes instead.

Still out of scope unless the roadmap changes: the LLM operations
assistant, a React dashboard, canary configuration rollout, and
multi-region or multi-cloud deployment. If a task seems to need one of
these, stop and ask.

## Before adding a new dependency
Ask first. State what stdlib/existing-dependency alternative was
considered and why it isn't enough. See `.agent/rules/code-style.md`.

## Migrations and schema
Never edit an existing migration. Schema changes go in a new numbered
migration file (next available number after `000009`).

## When something looks wrong or outdated in existing code
Flag it as its own short conversation before proposing a fix as a unit.
Don't silently "improve" code outside the current task's scope — see
`.agent/rules/workflow.md`.