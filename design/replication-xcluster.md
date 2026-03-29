# xCluster Apply

### Purpose
Applies CDC events to target cluster semantics with retry/backoff, deduplication, checkpoint advancement, and lag/status reporting.

### Scope
**In scope:**
- `xcluster.Loop` apply path (`ApplyEvent`, `ApplyBatch`) in `internal/replication/xcluster/apply.go`.
- Retry policy and apply statistics.
- Resume and lag/status helper APIs.

**Out of scope:**
- CDC event production and stream registration (CDC/control-plane features).

### Primary User Flow
1. Scheduler or caller sends CDC event batch to xCluster loop.
2. Loop filters duplicates, retries apply failures up to policy limits, and advances checkpoints after successful apply.
3. Caller reads loop stats/status for observability.

### System Flow
1. Entry: `Loop.ApplyBatch` iterates events and calls `ApplyEvent`.
2. `ApplyEvent` validates identifiers and sequence.
3. Fetches current checkpoint from `CheckpointStore`.
4. Duplicate guard:
   - event sequence <= checkpoint -> duplicate counter increment.
   - event dedupe key already in `appliedSet` -> duplicate counter increment.
5. Calls `applyWithRetry` on injected `Applier`.
6. On success, advances checkpoint and updates stats.

### Data Model
- Config:
  - `Config {Retry {MaxAttempts, Backoff}}`.
- Runtime stats:
  - `Stats {AppliedEvents, DuplicateEvents, RetryCount, FailureCount, LastAppliedSeq, LastUpdated}`.
- In-memory dedupe set:
  - `appliedSet map["stream|tablet|seq"]struct{}`.
- Persistence behavior:
  - checkpoint durability is delegated to injected `CheckpointStore`.

### Interfaces and Contracts
- `Applier.Apply(ctx, cdc.Event) error` must apply one event atomically from loop perspective.
- `CheckpointStore.GetCheckpoint/AdvanceCheckpoint` is required and called per event.
- Optional lag provider contract when store also implements `LagProvider`.

### Dependencies
**Internal modules:**
- `internal/common/errors` for retryable unavailable/validation errors.
- `internal/replication/cdc` event/checkpoint types.

**External services/libraries:**
- None directly; behavior delegated to injected applier and checkpoint store.

### Failure Modes and Edge Cases
- Missing stream/tablet IDs or zero sequence -> `ErrInvalidArgument`.
- Apply failure after retry budget exhaustion -> `ErrRetryableUnavailable`.
- Checkpoint store failures bubble directly.
- Duplicate events are silently ignored except stats/log updates.

### Observability and Debugging
- Log lines emitted for duplicate, failure, success, and resume-checkpoint paths.
- Inspect via `Loop.Stats(ctx)` and `Loop.Status(ctx)`.
- Test coverage:
  - `apply_test.go`, `apply_integration_test.go`, `contract_stability_test.go`, `apply_benchmark_test.go`.

### Risks and Notes
- `appliedSet` grows in memory with each unique event key and has no eviction path in current implementation.
- Checkpoint advancement is per-event, which can increase overhead for very large batches.

Changes:

- Keep xCluster explicitly optional and independent from local Raft HA assumptions in the simplified architecture.
- Revalidate dedupe/checkpoint behavior for ownership changes when the source no longer relies on Raft leader semantics.
