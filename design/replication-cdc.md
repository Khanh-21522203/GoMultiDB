# CDC Replication

### Purpose
Owns CDC stream event storage, checkpoint progression, stream service APIs, retention/bootstrap/split contracts, remote producer polling helpers, and CDC metrics bridging.

### Scope
**In scope:**
- Event/checkpoint store in `internal/replication/cdc/store.go`.
- Stream API service in `internal/replication/cdc/service.go`.
- Bootstrap/retention and split API helpers in `bootstrap.go` and `advanced_contracts.go`.
- Checkpoint durability file store in `checkpoint_file_store.go`.
- Split remapper and poller config remap in `split_remapper.go`.
- RPC producer and poller loop in `net_producer.go`.
- CDC-to-observability metric bridge in `metrics.go`.

**Out of scope:**
- Downstream xCluster apply semantics (`replication-xcluster.md`).
- Local Raft/consensus membership decisions.

### Primary User Flow
1. Producer appends ordered events per `(stream, tablet)`.
2. Consumer polls events after checkpoint and applies locally.
3. Consumer advances checkpoint; lag/status snapshots are queried for monitoring.
4. Control paths mark bootstrap required when retention floor surpasses checkpoint.
5. Tablet split remap copies parent checkpoint to children and updates poller configs.

### System Flow
1. Store path:
   - `AppendEvent` enforces monotonic sequence.
   - `Poll` returns events after `AfterSeq` up to `MaxRecords`.
   - `AdvanceCheckpoint` enforces non-regressing sequence.
2. Service path:
   - `CreateStream` registers `(streamID -> tabletID)` metadata.
   - `GetChanges` delegates to configured `Producer` (default `StoreProducer`).
   - `SetCheckpoint` validates stream/tablet mapping then advances checkpoint.
3. Bootstrap path:
   - `EvaluateRetentionAndMark` marks `BootstrapStateRequired` when checkpoint falls below retention floor.
4. Split path:
   - `SplitRemapper.RegisterSplit` copies parent checkpoint to left/right child.
5. RPC poller path:
   - `Poller.RunOnce` = load checkpoint -> remote `GetChanges` -> apply events -> advance checkpoint.
6. Ownership semantics:
   - CDC remains an optional async replication/changefeed path.
   - Source ordering is defined by monotonic `Sequence` per `(stream, tablet)` across ownership transitions.
   - Checkpoints are monotonic and non-regressing for the same `(stream, tablet)`.

### Data Model
- Core models (`model.go`):
  - `Event {StreamID, TabletID, Sequence, TimestampUTC, Payload}`.
  - `Checkpoint {StreamID, TabletID, Sequence, Timestamp}`.
  - `PollRequest/PollResponse`.
- Store metrics:
  - `Metrics {PollRequests, PollReturnedEvents, CheckpointAdvances, ...}`.
- Bootstrap state model:
  - `BootstrapStatus {StreamID, State, Reason, UpdatedAt}` with states `NONE/REQUIRED/IN_PROGRESS/COMPLETE/FAILED`.
- Persistence:
  - In-memory by default (`Store`).
  - Optional file persistence: `FileCheckpointStore` JSON map saved atomically via `<path>.tmp` rename.

### Interfaces and Contracts
- Store contracts:
  - sequence monotonicity for events and checkpoints.
  - duplicate equal-sequence appends/checkpoints treated as no-op metrics increments.
- Service contracts:
  - stream/tablet pair must match registered metadata.
  - unknown stream -> `ErrInvalidArgument`.
- RPC producer contracts:
  - service/method names `cdc.CDCProducer/GetChanges` and `SetCheckpoint`.
- Split remap contracts:
  - parent checkpoint copied to both children before registration completes.

### Dependencies
**Internal modules:**
- `internal/common/errors` for canonical failures.
- `internal/replication/observability` for histogram publication in `MetricsRegistry`.

**External services/libraries:**
- RPC transport via `internal/rpc.Client` for remote producer path.

### Failure Modes and Edge Cases
- Missing stream/tablet IDs -> `ErrInvalidArgument`.
- Event/checkpoint sequence regression -> `ErrConflict`.
- Producer not configured in service -> `ErrInternal`.
- Poller apply failure stops run with wrapped error; checkpoint is advanced only after apply success.
- Split remap on unknown parent in registry returns error.
- File checkpoint load/save errors return `ErrInternal` wrappers.

### Observability and Debugging
- Store logs append/checkpoint actions with sequence details.
- Status APIs:
  - `Store.Status`, `Store.LagSnapshot`, `Service.ListStreams`, `Service.BootstrapStatus`.
- Key debug points:
  - `store.go:AppendEvent/Poll/AdvanceCheckpoint`
  - `service.go:GetChanges/SetCheckpoint`
  - `net_producer.go:Poller.RunOnce`
- Coverage includes:
  - `store_test.go`, `service_test.go`, `bootstrap_test.go`, `advanced_contracts_test.go`, `split_remapper_test.go`, `checkpoint_file_store_test.go`, `contract_stability_test.go`, `advanced_contracts_test.go`.

### Risks and Notes
- In-memory store is not durable; file checkpoint store only persists checkpoints, not event backlog.
- `MetricsRegistry` encodes stream/tablet identity in metric names rather than label sets.
- If ownership transfer occurs, the new source owner must continue sequence progression from the previous owner; otherwise events are treated as duplicates/conflicts by consumers.

Changes:
