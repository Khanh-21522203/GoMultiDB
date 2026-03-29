# Tablet Snapshot Bootstrap

### Purpose
Provides tablet-level snapshot creation/deletion/restore plus remote bootstrap session transfer and destination-side file installation orchestration.

### Scope
**In scope:**
- Tablet snapshot store/service in `internal/tablet/snapshot`.
- Remote bootstrap source session manager in `internal/tablet/remotebootstrap/session.go`.
- Destination bootstrap client orchestration in `internal/tablet/remotebootstrap/client.go`.

**Out of scope:**
- Master-level distributed snapshot scheduling (master snapshot coordinator doc).

### Primary User Flow
1. Master or operator requests tablet snapshot RPC.
2. Tablet snapshot store scans DBs, persists snapshot metadata/data, and returns snapshot ID.
3. Remote bootstrap destination starts source session, streams chunks with CRC checks, installs staged files, and finalizes source session.

### System Flow
1. Snapshot RPC entry:
   - `create_tablet_snapshot`, `delete_tablet_snapshot`, `restore_tablet_snapshot` in `snapshot/service.go`.
2. Store create path (`Store.CreateSnapshot`):
   - scan `RegularDB` and `IntentsDB`
   - persist `snapshot/meta/<id>` and `snapshot/data/<id>/...` keys
   - register in-memory snapshot map.
3. Remote bootstrap source path:
   - `StartRemoteBootstrap` creates `SessionTransferring` with manifest.
   - `FetchFileChunk` serves chunk + CRC32.
   - `FinalizeBootstrap` marks finalized and releases session.
4. Destination client path (`Client.Run`):
   - `StartSession` -> `FetchManifest` -> chunked download + CRC verify -> install -> finalize or abort on failure.

### Data Model
- Snapshot data model (`SnapshotData`):
  - `SnapshotID`, `TabletID`, `CreatedAt`, `CreateHT`, `RegularData []KVPair`, `IntentsData []KVPair`.
- Remote bootstrap session model:
  - `SessionID`, `TabletID`, `SourcePeerID`, `StartedAt`, `State`, `Manifest`, `Staged map[file][]byte`.
- Manifest model:
  - `Manifest {TabletID, Files []FileMeta}`.
- Persistence keys:
  - `snapshot/meta/<snapshotID>`.
  - `snapshot/data/<snapshotID>/regular/<idx>` and `.../intents/<idx>`.
- Remote bootstrap session persistence: none (in-memory map).

### Interfaces and Contracts
- Tablet snapshot service methods:
  - `create_tablet_snapshot`, `delete_tablet_snapshot`, `restore_tablet_snapshot`.
- Source bootstrap contract (`Source` interface):
  - `StartSession`, `FetchManifest`, `FetchFileChunk`, `FinalizeBootstrap`, `AbortSession`.
- Destination installer contract (`Installer.Install`) must apply staged files atomically.

### Dependencies
**Internal modules:**
- `internal/storage/rocks` for snapshot scans and restore write batches.
- `internal/common/errors` for argument/conflict/internal error mapping.

**External services/libraries:**
- CRC32 integrity checks via `hash/crc32`.

### Failure Modes and Edge Cases
- Empty snapshot or tablet IDs -> `ErrInvalidArgument`.
- Duplicate snapshot create -> `ErrConflict`.
- Restore/delete unknown snapshot -> `ErrInvalidArgument`.
- Remote bootstrap session cap exceeded -> `ErrRetryableUnavailable`.
- Chunk checksum mismatch -> `ErrConflict`.
- Destination client aborts source session on manifest/chunk/install errors.
- Stale session cleanup by `ExpireStaleSessions` transitions to failed and removes session.

### Observability and Debugging
- Debug points:
  - `snapshot/store.go:CreateSnapshot/RestoreSnapshot`
  - `remotebootstrap/session.go:FetchFileChunk/FinalizeWithValidation`
  - `remotebootstrap/client.go:Run`
- Tests:
  - `internal/tablet/snapshot/store_test.go`, `service_test.go`
  - `internal/tablet/remotebootstrap/session_test.go`, `client_test.go`

### Risks and Notes
- Snapshot persistence currently stores copied data in same Rocks keyspace with `snapshot/` prefixes; large snapshots can increase in-memory store footprint.
- Session finalize behavior differs between source-only cleanup (`FinalizeBootstrap`) and in-process validation path (`FinalizeWithValidation`).

Changes:

