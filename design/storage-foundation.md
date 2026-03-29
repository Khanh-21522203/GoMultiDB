# Storage Foundation

### Purpose
Provides the low-level persistence substrate: a Rocks-like KV abstraction (`RegularDB` + `IntentsDB`), segmented WAL, and DocDB MVCC/intents logic built on that KV contract.

### Scope
**In scope:**
- In-memory KV store contract in `internal/storage/rocks`.
- WAL append/recover/rotate behavior in `internal/wal/log.go`.
- DocDB non-transactional writes, intent writes, apply/remove intents, and MVCC reads in `internal/docdb/engine.go`.

**Out of scope:**
- Higher-level transaction orchestration and conflict policy (txn feature).

### Primary User Flow
1. Caller writes non-transactional DocDB mutations or transactional intents.
2. WAL persists ordered replication entries.
3. Commit path applies intents to regular versioned keys; abort path removes intents.
4. Readers use `ReadAt` hybrid-time visibility rules.

### System Flow
1. KV contract:
   - `ApplyWriteBatch` writes/deletes keys (`nil` value => delete).
   - `Get` and `NewIterator` provide point and prefix scans.
2. WAL path:
   - async append queue -> `appendAndSync`.
   - JSON-line records written to `segment-XXXXXX.wal`.
   - segment rotation on `MaxSegmentBytes`.
   - startup recovery scans segment files and rebuilds op index.
3. DocDB path:
   - `ApplyNonTransactional` writes `key|v|<HT>` values.
   - `WriteIntents` writes intent key + reverse index in `IntentsDB`.
   - `ApplyIntents` converts reverse-indexed intents into regular committed versions and deletes intent keys.
   - `ReadAt` scans version prefix and picks highest visible HT <= readHT.

### Data Model
- Rocks models:
  - `KV {Key, Value}`, `WriteBatch {Ops []KV}`, `DBKind {RegularDB, IntentsDB}`.
- WAL model:
  - `Entry {OpID, HybridTime, Payload}`.
- DocDB models:
  - `KVMutation {Key, Value, Op, WriteHT}`.
  - `IntentRecord {TxnID, Key, Value}`.
- Key formats:
  - versioned regular key: `<user-key>|v|<20-digit-ht>`.
  - intent key: `<txn-id-bytes>|<user-key>`.
  - reverse index key: `ri|<txn-id-hex>|<user-key>`.

### Interfaces and Contracts
- `rocks.Store` interface is shared storage boundary used by multiple features.
- WAL contracts:
  - `AppendSync(ctx, entries)` waits for append callback.
  - `ReadFrom(from,max)` returns in-memory recovered/append order sequence.
  - `IndexLookup(opID)` resolves segment and offset.
- DocDB contracts:
  - `ApplyIntents`/`RemoveIntents` support bounded batch (`limit`) and return `(done, err)`.

### Dependencies
**Internal modules:**
- `internal/common/types` for WAL `OpID`.
- `internal/storage/rocks` consumed by DocDB and several higher layers.

**External services/libraries:**
- Filesystem for WAL segment and metadata files.

### Failure Modes and Edge Cases
- WAL startup fails if directory is missing/invalid segment naming/unmarshal errors.
- WAL async queue saturation returns immediate `queue full` error.
- DocDB rejects unsupported mutation op.
- Intent apply/remove path bubbles iterator/get/unmarshal/store errors.
- `ReadAt` returns not-found when no version <= readHT exists.

### Observability and Debugging
- Debug entry points:
  - `wal/log.go:appendAndSync/recoverAll`
  - `docdb/engine.go:ApplyIntents/ReadAt`
  - `storage/rocks/store.go:ApplyWriteBatch/NewIterator`
- Tests:
  - `internal/storage/rocks/store_test.go`
  - `internal/wal/log_test.go`
  - `internal/docdb/engine_test.go`

### Risks and Notes
- `rocks.MemoryStore` is an in-memory implementation; production durable RocksDB wiring is not present in this repo.
- WAL indexes and entry cache are process-memory structures rebuilt at restart from segment replay.

Changes:

