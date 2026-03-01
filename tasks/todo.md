# Phase 5 Kickoff — SQL/CQL Query Layers (Pragmatic)

## Context

- Phase 4 control-plane scaffolding is in place and green.
- Deferred depth items are tracked in `tasks/deferred.md`.
- Phase 5 starts with minimal integration surfaces before deep protocol compatibility.

## P5.1 YSQL Process Control Skeleton (first)

- [x] Add `internal/query/ysql` package with process coordinator interface and local stub implementation.
- [x] Add start/stop/health lifecycle API aligned with `plan-ysql-query-layer.md`.
- [x] Integrate coordinator hooks into tserver runtime entrypoints (feature-flag guarded, no external PG process yet).
- [x] Add focused unit tests for runtime integration and health semantics.
- [x] Lint clean + `go test ./...` passing.

## P5.2 PgGate Bridge Session Skeleton

- [x] Add `internal/query/pggate` package with session model (`session id`, `catalog version`, `table cache`, `pending writes`).
- [x] Add minimal operation structs for read/write requests (shape only; no deep execution).
- [x] Add request validation + idempotency-safe session operations.
- [x] Add focused unit tests for session lifecycle and cache invalidation triggers.
- [x] Lint clean + `go test ./...` passing.

## P5.3 YCQL Server Surface Skeleton

- [x] Add `internal/query/ycql` package with server config + lifecycle stubs.
- [x] Add minimal request routing surface for future protocol handling.
- [x] Add focused tests for startup/config validation and health.
- [x] Lint clean + `go test ./...` passing.

## P5.4 Transaction Boundaries & Runtime Control Surface

- [x] Add minimal PgGate transaction boundary skeleton (`BeginTxn`, `CommitTxn`, `AbortTxn`) bound to session state.
- [x] Add explicit YSQL runtime control/status API (`StartYSQL`, `StopYSQL`, `GetYSQLStatus`) per plan interface.
- [x] Add focused tests for transaction state transitions and runtime status semantics.
- [x] Lint clean + `go test ./...` passing (validated from terminal output).

## P5.5 YCQL Prepared Statement Session Skeleton

- [x] Add YCQL session manager with per-connection state (`prepared`, `keyspace`, `consistency`, `schema version`).
- [x] Add minimal `Prepare` + `ExecutePrepared` flow with schema-version conflict signaling.
- [x] Add explicit prepared invalidation on schema version change.
- [x] Add focused unit tests for session lifecycle, prepare/execute, and invalidation/conflict paths.
- [x] Lint clean + `go test ./...` passing (validated from terminal output).

## P5.6 Batch + Savepoint Skeleton Hardening

- [x] Add YCQL batch request routing surface with validation (`RouteBatch`) and focused tests.
- [x] Add PgGate savepoint primitives (`CreateSavepoint`, `RollbackToSavepoint`, `ReleaseSavepoint`) with focused tests.
- [x] Fix RPC start goroutine nil-deref race in `internal/rpc/server.go` by serving with captured local server pointer.
- [x] Lint clean + `go test ./...` passing (validated from terminal output).

## P5.7 Bridge/API Execution Skeleton

- [x] Add PgGate minimal read/write execution entrypoints that consume current session + txn state (no storage engine integration yet).
- [x] Add YCQL session binding into server path (connection-scoped session lookup + prepared execute dispatch).
- [x] Add focused tests for execution-surface validation and idempotent behavior.
- [x] Lint clean + `go test ./...` passing (validated from terminal output).

## P5.8 Retry & Prepared Metrics Skeleton

- [x] Add PgGate retry/restart conflict counters on read/write skeleton conflict paths.
- [x] Add YCQL prepared cache stats (hit/miss/invalidation) with accessors.
- [x] Add focused unit tests for counters and conflict paths.
- [x] Lint clean + `go test ./...` passing (validated from terminal output).

## P5.9 Idempotency Hooks & Connection Guardrails

- [x] Add PgGate write-path idempotency hook (`ExecWriteWithRequest`) with fingerprint conflict handling.
- [x] Add YCQL connection limit guardrail (`MaxConnections`) with open/close semantics.
- [x] Add focused tests for idempotency conflict and connection cap behavior.
- [x] Lint clean + `go test ./...` passing (validated from terminal output).

## P5.10 Read Idempotency + YCQL Status Snapshot

- [x] Add PgGate read-path idempotency hook (`ExecReadWithRequest`) with fingerprint conflict handling.
- [x] Add YCQL status snapshot API (started, max connections, active connections, prepared stats).
- [x] Add focused tests for read idempotency and status reporting semantics.
- [x] Lint clean + `go test ./...` passing (validated from terminal output).

## P5 Exit for this iteration (pragmatic)

- [x] YSQL/PgGate/YCQL package boundaries established.
- [x] Lifecycle and session scaffolding test-backed.
- [x] No protocol compatibility claims yet (next bounded iterations).
