# Master Snapshot Coordinator

### Purpose
Coordinates distributed snapshot create/delete/restore operations across tablets and persists snapshot descriptors for restart recovery.

### Scope
**In scope:**
- Snapshot lifecycle and fan-out orchestration in `internal/master/snapshot/coordinator.go`.
- Snapshot master RPC surface in `internal/master/snapshot/service.go`.
- Tablet RPC client/registry bridge in `internal/master/snapshot/client.go`.
- Durable snapshot metadata store in `internal/master/snapshot/store.go`.

**Out of scope:**
- Tablet-local snapshot data copy/restore implementation (`tablet/snapshot`).

### Primary User Flow
1. Client calls master snapshot RPC (`create_snapshot`, `delete_snapshot`, `restore_snapshot`).
2. Coordinator validates snapshot state and fan-outs per-tablet RPC calls.
3. Coordinator updates/persists snapshot state transitions.
4. Caller can inspect snapshot by `get_snapshot` or `list_snapshots`.

### System Flow
1. RPC entry methods in `Service.Methods()` dispatch to coordinator APIs.
2. `Coordinator.CreateSnapshot`:
   - creates `SnapshotInfo` in `StateCreating`
   - persists via `SnapshotStore.SaveSnapshot`
   - fan-outs `CreateTabletSnapshot` calls via bounded semaphore after primary-owner endpoint lookup through `RegistryClient`.
3. Success transitions to `StateComplete`; failures set `StateFailed` and `Error`.
4. `DeleteSnapshot` transitions complete snapshot to deleting, fan-outs delete RPCs, and removes descriptor on success.
5. `Recover` loads persisted snapshots and resumes in-progress states (`CREATING`, `RESTORING`, `DELETING`).

### Data Model
- `SnapshotInfo`:
  - `SnapshotID`, `NamespaceIDs`, `TableIDs`, `TabletIDs`, `CreateHT`, `State`, `CreatedAt`, `Error`.
- `State` enum: `CREATING`, `COMPLETE`, `FAILED`, `DELETING`, `RESTORING`.
- Persistence keys (master snapshot store):
  - `snapshot/<snapshot_id>` -> JSON `SnapshotInfo`.
- In-memory map: `Coordinator.snapshots map[string]*SnapshotInfo`.

### Interfaces and Contracts
- Coordinator public API:
  - `CreateSnapshot(ctx, snapshotID, tabletIDs, createHT)`
  - `DeleteSnapshot(ctx, snapshotID)`
  - `RestoreSnapshot(ctx, snapshotID)`
  - `GetSnapshot(snapshotID)` / `ListSnapshots()` / `Recover(ctx)`
- Tablet RPC contract (`TabletSnapshotRPC`):
  - `CreateTabletSnapshot`, `DeleteTabletSnapshot`, `RestoreTabletSnapshot`.
- Master RPC methods:
  - `snapshot.create_snapshot`, `delete_snapshot`, `restore_snapshot`, `get_snapshot`, `list_snapshots`.

### Dependencies
**Internal modules:**
- `internal/common/errors` for typed failures.
- `internal/storage/rocks` via `RocksSnapshotStore`.

**External services/libraries:**
- HTTP RPC calls to tablet endpoints through `snapshot.Client` and registry lookup.

### Failure Modes and Edge Cases
- Empty snapshot ID or empty tablet list -> `ErrInvalidArgument`.
- Duplicate snapshot ID -> `ErrConflict`.
- Delete/restore on non-`COMPLETE` states -> `ErrConflict`.
- Registry endpoint lookup can fail with `ErrPrimaryOwnerChanged` when primary ownership metadata is stale; caller should refresh and retry.
- Fan-out failures mark snapshot `FAILED` for create/restore; delete leaves state for retry.
- Recovery attempts best-effort resume for in-progress snapshots.

### Observability and Debugging
- Key points:
  - `coordinator.go:CreateSnapshot/DeleteSnapshot/RestoreSnapshot/Recover`
  - `service.go` RPC wrappers
  - `client.go:call` transport failures
- Coverage:
  - `coordinator_test.go`, `service_test.go`, `client_test.go`, `e2e_test.go`.

### Risks and Notes
- Fan-out returns first observed error; partial tablet success is possible and represented via failed snapshot state.
- Registry endpoint selection now targets heartbeat-derived primary ownership; primary transitions require metadata freshness for low retry latency.

Changes:
