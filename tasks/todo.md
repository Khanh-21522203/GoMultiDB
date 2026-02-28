# Phase 4 Checkpoint — Metadata & Control Plane (Pragmatic)

## Completed

- [x] Catalog foundation with leader-gated, idempotent `CreateTable`/`GetTable`.
- [x] Table state-transition guardrails in catalog manager.
- [x] Heartbeat + TS registry skeleton with restart detection and tablet-report sequencing.
- [x] Catalog reconciliation bridge (`ApplyTabletReport`) with leader gating.
- [x] Concrete in-memory reconcile sink for tablet placement/status.
- [x] Basic directive planner (`CREATE_TABLET` / `DELETE_TABLET`) using target RF rules.
- [x] Planner output wired into heartbeat `TabletActions`.
- [x] Per-reporter action filtering (only tablets from current report entries).
- [x] TS liveness query (`ListStale`) with timeout-based stale detection.
- [x] Stale-aware planning policy hooks:
  - ignore stale replicas in planning input,
  - skip directive generation on heartbeats from reporters that were stale.
- [x] Focused unit tests added across catalog + heartbeat flows.
- [x] Lint clean and `go test ./...` passing.

## Deferred (intentional for side-project scope)

Moved to `tasks/deferred.md` for centralized tracking across phases.

## Recommendation

- Phase 4 is **done enough** for pragmatic scope and safe to continue to Phase 5 scaffolding.
