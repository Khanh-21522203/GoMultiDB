# Replication Control Plane

### Purpose
Maintains stream/job administrative state and runs scheduler ticks that poll CDC and dispatch batches to xCluster with in-flight caps and deterministic fault injection hooks.

### Scope
**In scope:**
- Stream/job registry in `internal/replication/controlplane/registry.go`.
- Persistent registry file load/save logic.
- Scheduler orchestrator in `internal/replication/controlplane/scheduler.go`.

**Out of scope:**
- CDC event storage internals and xCluster apply internals.

### Primary User Flow
1. Operator/control code creates streams and jobs.
2. Registry transitions states (`RUNNING`, `PAUSED`, `STOPPED`) for streams and jobs.
3. Scheduler tick polls running stream/jobs, respects in-flight cap, dispatches events to xCluster loop.
4. Admin snapshots combine registry, CDC checkpoint/lag, and xCluster stats.

### System Flow
1. Registry APIs mutate maps and persist envelope to file when file-backed mode is enabled.
2. `Scheduler.Tick`:
   - optional injected delay/fault via `maybeInjectTransportFault`.
   - list streams/jobs and filter to runnable pairs.
   - enforce per-job in-flight cap.
   - poll CDC after stream checkpoint.
   - apply batch via `xcluster.Loop.ApplyBatch`.
3. Snapshot path (`Registry.Snapshot`) enriches stream state with checkpoint/lag from CDC and apply stats from xCluster loop.

### Data Model
- `Stream`:
  - `ID`, `TabletID`, `State`, `CreatedAt`, `UpdatedAt`, `Checkpoint`, `LagEvents`.
- `Job`:
  - `ID`, `StreamID`, `Target`, `State`, `CreatedAt`, `UpdatedAt`.
- `Snapshot`:
  - `GeneratedAt`, `Streams`, `Jobs`, `Apply xcluster.Stats`.
- Persistence file model:
  - `persistenceEnvelope {Version, Streams, Jobs}` stored as JSON at registry path.

### Interfaces and Contracts
- Registry APIs:
  - `CreateStream`, `PauseStream`, `ResumeStream`, `StopStream`.
  - `CreateJob`, `PauseJob`, `ResumeJob`, `StopJob`.
  - `ListStreams`, `ListJobs`, `Snapshot`.
- Scheduler API:
  - `Tick(ctx)` performs one scheduling cycle.
  - `InFlight(ctx, jobID)` returns current in-flight count.
- State transition contract:
  - stopped stream/job cannot transition back to active states.

### Dependencies
**Internal modules:**
- `internal/replication/cdc` for checkpoint and polling.
- `internal/replication/xcluster` for batch apply and stats.
- `internal/common/errors` for argument/conflict/retryable errors.

**External services/libraries:**
- Filesystem persistence for registry JSON file.

### Failure Modes and Edge Cases
- Creating job for unknown stream -> `ErrInvalidArgument`.
- Invalid state transitions from stopped state -> `ErrConflict`.
- Scheduler can intentionally return `ErrRetryableUnavailable` on configured fault cadence.
- CDC polling or xCluster apply errors abort current tick and bubble up.

### Observability and Debugging
- Debug points:
  - `registry.go:setStreamState/setJobState/saveLocked`
  - `scheduler.go:Tick`
- Coverage:
  - `registry_test.go`, `scale_test.go`
  - `scheduler_test.go`, `robustness_test.go`

### Risks and Notes
- Scheduler is synchronous per tick and can become bottleneck if many jobs have large batches.
- Fault injection in scheduler is deterministic by tick count, useful for tests but not dynamic runtime policy.

Changes:

