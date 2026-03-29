# Tablet Lifecycle

### Purpose
Controls tablet state transitions (create/split/delete/remote-bootstrap), tracks ownership metadata/epochs, supports explicit two-phase ownership transfer, persists lifecycle markers to disk, and maintains key-range routing via the partition map.

### Scope
**In scope:**
- Tablet manager state machine in `internal/tablet/manager.go`.
- Metadata persistence contract in `internal/tablet/meta_store.go`.
- Partition routing and split registration in `internal/partition/map.go`.

**Out of scope:**
- Snapshot copy mechanics (`tablet/snapshot`) and bootstrap file transfer protocol (`tablet/remotebootstrap`).

### Primary User Flow
1. Tablet is created with table metadata and partition bounds.
2. Tablet transitions through running/tombstone/delete or split paths.
3. Ownership changes are coordinated as two-phase transfer (`TransferPrepare` -> `TransferCommit`) with owner epoch bumps.
4. Split operation creates child tablets inheriting owner metadata, updates partition map, and tombstones parent.
5. On startup, manager recovers persisted `.meta` markers, normalizes incomplete states, and clears non-committed prepared ownership transfers.

### System Flow
1. Entry: `Manager.CreateTablet` validates metadata and writes meta file before exposing peer in memory.
2. Delete path:
   - tombstone mode -> transition to `TOMBSTONED` and persist.
   - hard delete -> `DELETING`, remove meta file, mark `DELETED`, remove peer.
3. Ownership transfer path:
   - `TransferPrepare` validates source owner/epoch + target owner and writes `TRANSFER_PREPARED` metadata.
   - `TransferCommit` commits pending owner and bumps `OwnerEpoch`.
4. Split path (`SplitTabletWithExpectedStateVersion`):
   - validate split bounds
   - persist parent splitting + child metas
   - register split in `partition.Map`
   - tombstone parent.
5. Remote bootstrap transition path allows `FAILED` or `TOMBSTONED` tablet back to `RUNNING`, clears transfer-prepared state, and updates owner/epoch when source peer changes.

```
CreateTablet -> RUNNING
RUNNING --transfer_prepare--> RUNNING(TRANSFER_PREPARED)
RUNNING(TRANSFER_PREPARED) --transfer_commit--> RUNNING(owner epoch++)
RUNNING --split--> SPLITTING -> child RUNNING + parent TOMBSTONED
RUNNING --delete--> TOMBSTONED or DELETING -> DELETED
FAILED/TOMBSTONED --remote bootstrap--> RUNNING
```

### Data Model
- `tablet.Meta`:
  - `TabletID`, `TableID`, `Partition (StartKey/EndKey)`, `SplitParentID`, `SplitDepth`.
  - Ownership fields: `OwnerTSUUID`, `OwnerEpoch`, `TransferState`, `TransferEpoch`, `PendingOwner`.
  - Lifecycle fields: `State`, `StateVersion`.
- `tablet.Peer`:
  - `Meta`, `State`, `LastError`.
- `partition.TabletPartition`:
  - `TabletID`, `Bound`, `State` (`RUNNING` or `SPLIT`).
- Persistence path:
  - `<metaDir>/<tablet-id>.meta` (JSON-encoded `Meta`) via `FileMetaStore` atomic temp-write + rename.

### Interfaces and Contracts
- Tablet manager APIs:
  - `CreateTablet`, `OpenTablet`, `DeleteTablet*`, `SplitTablet*`, `RemoteBootstrapTablet*`.
  - Ownership APIs: `TransferPrepare`, `TransferCommit`.
  - Read API: `ListTablets`.
- State version contract:
  - Optional precondition `expectedStateVersion` prevents stale callers from mutating state.
- Owner epoch contract:
  - Optional precondition `expectedOwnerEpoch` on `TransferPrepare` enforces stale-owner protection.
- Partition map APIs:
  - `FindTablet(key)`, `ListOverlapping(start,end)`, `RegisterTabletSplit(parent,left,right)`.

### Dependencies
**Internal modules:**
- `internal/common/errors` for transition/argument/conflict errors.
- `internal/partition` for split/routing metadata updates.

**External services/libraries:**
- Filesystem operations (`os`, `filepath`) for durable meta markers.

### Failure Modes and Edge Cases
- Invalid transition attempts return `ErrConflict`.
- Concurrent operation guard (`ops` map) returns `ErrConflict` for same-tablet overlapping ops.
- Split key outside parent range returns `ErrInvalidArgument`.
- Split is rejected when ownership transfer is in prepared state.
- Transfer prepare/commit source owner, owner epoch, transfer epoch, and state-version mismatches return `ErrConflict`.
- Partition map registration failure triggers split rollback (children removed and parent reverted best-effort).
- Recovery marks incomplete startup states (`BOOTSTRAPPING`, `SPLITTING`, `REMOTE_BOOTSTRAPPING`) as `FAILED`.
- Recovery clears non-committed transfer-prepared metadata to require fresh master-driven prepare.

### Observability and Debugging
- Main points:
  - `manager.go:SplitTabletWithExpectedStateVersion`
  - `manager.go:RemoteBootstrapTabletWithExpectedStateVersion`
  - `meta_store.go:WriteMeta`
  - `partition/map.go:FindTablet/RegisterTabletSplit`
- Tests:
  - `internal/tablet/manager_test.go`
  - `internal/tablet/manager_internal_test.go`
  - `internal/tablet/manager_fs_test.go`
  - `internal/partition/map_test.go`

### Risks and Notes
- Recovery writes best-effort state conversions for incomplete operations; persistent write failures during recovery are tolerated in some branches.
- Partition map is in-memory and must be kept consistent with tablet manager events by caller orchestration.
- Transfer semantics are deterministic but still non-consensus; atomicity depends on ordered `TransferPrepare`/`TransferCommit` orchestration.

Changes:
