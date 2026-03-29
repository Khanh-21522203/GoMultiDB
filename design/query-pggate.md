# PgGate

### Purpose
Implements PgGate session state, transactional write buffering, read/write execution stubs, idempotent request semantics, and optional bridge interfaces to real tablet dispatch and distributed txn coordinator.

### Scope
**In scope:**
- Session and transaction state in `internal/query/pggate/session.go`.
- Exec read/write paths and retry stats in `executor.go`.
- External bridge interfaces (`TabletDispatcher`, `PartitionResolver`, `TxnCoordinator`) in `dispatch.go`.

**Out of scope:**
- SQL parser/planner.
- Actual tablet storage operations (injected via interfaces).

### Primary User Flow
1. Caller opens PgGate session with request ID.
2. Caller begins transaction (optional), queues writes, runs reads, creates savepoints.
3. Caller commits or aborts transaction; with bridge injection this dispatches to real coordinator/tablets.
4. Caller closes session.

### System Flow
1. Entry methods:
   - `OpenSession(ctx, reqID)` creates `Session` keyed by `pggate-<reqID>`.
   - `ExecWrite*` validates input, optional idempotency mark, queues write, optional flush.
   - `ExecRead*` validates input and optionally dispatches to tablet bridge.
2. Transaction methods:
   - `BeginTxn` creates `TxnHandle` (stub) or delegates to `TxnCoordinator.Begin`.
   - `CommitTxn` flushes writes and calls `TxnCoordinator.Commit` when real bridge is configured.
   - `AbortTxn` calls coordinator abort path when configured.
3. Savepoints:
   - `CreateSavepoint`, `RollbackToSavepoint`, `ReleaseSavepoint` operate on pending writes and op sequence.

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

### Observability and Debugging
- Stats surface: `RetryStats(ctx)` exposes read/write restart conflict counters.
- Debug entry points:
  - `session.go:BeginTxn/CommitTxn`
  - `executor.go:ExecWriteWithRequest`
  - `dispatch.go:flushWritesReal/commitTxnReal`
- Tests cover contracts and failover semantics:
  - `session_test.go`, `executor_test.go`, `dispatch_test.go`, `failover_semantics_test.go`.

### Risks and Notes
- Default behavior is scaffold-style for many reads/writes when real bridge dependencies are not injected.
- Idempotent replay currently accepts identical fingerprints but still queues duplicate writes in current skeleton behavior.

Changes:

- Finish PgGate implementation and remove scaffold-only behavior in read/write execution paths.
- Complete real tablet dispatch, partition resolution, and distributed transaction bridge flow as default behavior.
