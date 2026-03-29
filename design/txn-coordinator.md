# Transaction Coordinator

### Purpose
Owns distributed transaction record lifecycle, conflict resolution policy against intents, and wait-queue deadlock detection/victim selection.

### Scope
**In scope:**
- Transaction manager in `internal/txn/manager.go`.
- Conflict resolver in `internal/txn/conflict/resolver.go`.
- Wait queue and deadlock detector in `internal/txn/waitq`.

**Out of scope:**
- Intent storage layout details in DocDB (covered in storage feature).

### Primary User Flow
1. Coordinator begins txn with request ID and optional metadata.
2. Caller heartbeats/registers touched tablets.
3. Commit applies intents in batches and transitions txn to committed.
4. Abort removes intents and transitions to aborted.
5. Conflicts can abort lower-priority txns, trigger restart, or wait-queue behavior.

### System Flow
1. `Manager.Begin` creates `Record` in `Pending` state keyed by `TxnID` with request-id dedupe semantics.
2. `Commit` validates idempotency and liveness, sets `Committing`, calls `IntentApplier.ApplyIntents` loop, then marks `Committed`.
3. `Abort` sets `Aborting`, calls `IntentApplier.RemoveIntents` loop, then marks `Aborted`.
4. `ExpireStale` aborts pending/completing txns past timeout.
5. `conflict.Resolver.Check` evaluates intent owner status and applies decision matrix:
   - committed newer than snapshot -> restart required
   - pending lower/equal priority -> abort conflicting txn and retry
   - higher-priority blocker -> caller conflict
   - wait-queue mode -> retryable wait signal
6. `waitq.DeadlockDetector` periodically scans wait graph cycles and aborts deterministic victim.

### Data Model
- `txn.Record`:
  - `TxnID`, `State`, `CommitHT`, `Priority`, `StartHT`, `Isolation`, `StatusTablet`, `InvolvedTablets`, `LastHeartbeat`, `RequestID`.
- Conflict models:
  - `Intent {TxnID, Priority}`.
  - `StatusResult {Status, CommitHT, Priority}`.
- Wait-queue model:
  - `graph map[waiter]set(blockers)`, `notif map[waiter]chan struct{}`.
- Persistence: none in this package; manager records are in-memory.

### Interfaces and Contracts
- Manager APIs:
  - `Begin`, `RegisterTablet`, `Heartbeat`, `Commit`, `Abort`, `ExpireStale`, `Get`.
- Intent applier contract:
  - `ApplyIntents(txnID, commitHT, limit)` and `RemoveIntents(txnID, limit)` called until done.
- Conflict resolver dependencies:
  - `IntentScanner`, `StatusSource`, `AbortFn`.
- Wait queue contracts:
  - `Enqueue(waiter, blocker)` returns signal channel.
  - `Release(txnID)` unblocks waiters and removes txn edges.

### Dependencies
**Internal modules:**
- `internal/common/errors` for canonical txn/conflict/retryable codes.
- `internal/common/ids` for typed transaction and request identifiers.

**External services/libraries:**
- None beyond in-memory synchronization/time utilities.

### Failure Modes and Edge Cases
- Request-ID mismatch on existing txn -> `ErrIdempotencyConflict`.
- Commit on expired txn -> `ErrTimeout` and txn moved to aborted.
- Commit/abort apply loop errors -> `ErrRetryableUnavailable`.
- Conflict resolver status lookup failure -> `ErrRetryableUnavailable`.
- Wait queue victim selection ties break by lexicographically smallest txn ID.

### Observability and Debugging
- Primary debug points:
  - `manager.go:Commit/Abort/ExpireStale`
  - `conflict/resolver.go:Check`
  - `waitq/queue.go:DetectCycles` and `waitq/detector.go:detect`
- Tests:
  - `internal/txn/manager_test.go`
  - `internal/txn/conflict/resolver_test.go`
  - `internal/txn/waitq/queue_test.go`

### Risks and Notes
- Txn manager state is process-local; restart persistence and distributed ownership are not implemented in this package.
- Wait mode in conflict resolver is represented as retryable unavailable signal and requires higher-level orchestration to block/retry correctly.

Changes:

