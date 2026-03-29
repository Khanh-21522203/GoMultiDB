# PgGate

### Purpose
Implements PgGate session state, transactional write buffering, idempotent request semantics, and a default in-process bridge for partition resolution, tablet dispatch, and distributed transaction coordination.

### Scope
**In scope:**
- Session and transaction state in `internal/query/pggate/session.go`.
- Exec read/write paths and retry stats in `executor.go`.
- Bridge interfaces and default local bridge implementations in `dispatch.go` and `local_bridge.go`.

**Out of scope:**
- SQL parser/planner.
- Cross-node/master-driven routing and RPC dispatch (current default bridge is single-node in-process).

### Primary User Flow
1. Caller opens PgGate session with request ID.
2. Caller begins transaction (optional), queues writes, runs reads, creates savepoints.
3. Caller commits or aborts transaction; default bridge dispatches reads/writes and transaction lifecycle through concrete resolver/dispatcher/coordinator implementations.
4. Caller closes session.

### System Flow
1. Entry methods:
   - `OpenSession(ctx, reqID)` creates `Session` keyed by `pggate-<reqID>`.
   - `ExecWrite*` validates input, optional idempotency mark, queues write, optional flush.
   - `ExecRead*` validates input and dispatches through resolver + tablet dispatcher.
2. Transaction methods:
   - `BeginTxn` delegates to `TxnCoordinator.Begin`.
   - `CommitTxn` flushes writes and calls `TxnCoordinator.Commit`.
   - `AbortTxn` calls `TxnCoordinator.Abort`.
3. Savepoints:
   - `CreateSavepoint`, `RollbackToSavepoint`, `ReleaseSavepoint` operate on pending writes and op sequence.
4. Default bridge wiring:
   - `NewManager()` injects local implementations for `PartitionResolver`, `TabletDispatcher`, and `TxnCoordinator`.
   - If bridge dependencies are explicitly unset, read/write/txn operations fail fast with retryable unavailable errors instead of silently using stub behavior.

### Data Model
- `Session`:
  - `SessionID`, `CatalogVersion`, `Txn *TxnHandle`, `TableCache`, `PendingWrites []WriteOp`.
- `TxnHandle`:
  - `TxnID`, `State`, `Epoch`, `OpSeq`, `SnapshotHT`, `Savepoints []Savepoint`.
- `WriteOp`:
  - `TableID`, `Operation`, `Columns map[int]PgValue`, `BindVars`.
- `ReadOp`:
  - `TableID`, `IndexID`, `BindVars`, `Targets`, `FetchSize`.
- `ExecResponse`:
  - `Applied`, `Writes`, `Rows`, `TxnID`, `InTxn`, `Flushed`, `OpSeq`, `Conflict`.
- Default bridge state:
  - Local tablet shard mapping from table/bind vars.
  - In-memory per-table row counters in `localTabletDispatcher`.
  - `txn.Manager`-backed transaction tracking in `localTxnCoordinator`.
- Persistence: none; manager state is in-memory.

### Interfaces and Contracts
- Session APIs:
  - `OpenSession`, `CloseSession`, `GetSession`.
- Execution APIs:
  - `ExecWrite`, `ExecWriteWithRequest`, `ExecRead`, `ExecReadWithRequest`.
- Bridge contracts:
  - `PartitionResolver.Resolve(tableID, bindVars) -> tabletID`.
  - `TabletDispatcher.TabletWrite/TabletRead` for real I/O.
  - `TxnCoordinator.Begin/Commit/Abort` for distributed txn control.
- Idempotency contract:
  - request-scoped fingerprint mismatch returns `ErrIdempotencyConflict`.

### Dependencies
**Internal modules:**
- `internal/common/errors` for canonical errors.
- `internal/common/idempotency` for TTL-based request dedupe.
- `internal/common/ids` for request and txn IDs.

**External services/libraries:**
- None directly; external effects are abstracted through bridge interfaces.

### Failure Modes and Edge Cases
- Missing session ID, table ID, operation -> `ErrInvalidArgument`.
- Retry conflict sentinel paths (`RETRY_CONFLICT`) return `ErrConflict` and increment stats.
- Savepoint operations without active transaction return `ErrConflict`.
- Commit/abort with invalid txn ID hex in real bridge path returns `ErrInvalidArgument`.
- Bridge resolver/dispatcher/coordinator failures bubble to caller.
- Explicitly missing bridge dependencies return `ErrRetryableUnavailable` (no stub fallback path).

### Observability and Debugging
- Stats surface: `RetryStats(ctx)` exposes read/write restart conflict counters.
- Debug entry points:
  - `session.go:BeginTxn/CommitTxn`
  - `executor.go:ExecWriteWithRequest`
  - `dispatch.go:flushWritesReal/commitTxnReal`
- Tests cover contracts and failover semantics:
  - `session_test.go`, `executor_test.go`, `dispatch_test.go`, `failover_semantics_test.go`.

### Risks and Notes
- Default bridge semantics are single-node and in-memory; they provide deterministic execution flow but do not represent full multi-node routing semantics yet.
- Idempotent replay currently accepts identical fingerprints but still queues duplicate writes in current skeleton behavior.

Changes:
