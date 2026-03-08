# Implementation Todo

> **Source of truth**: all feature specs live in `docs/plans/`. This file cross-references them
> against the current code state and provides step-by-step implementation guidance.
> Security (TLS/auth/encryption) is explicitly out of scope for Phase 1.

---

## How to read this file

Each section maps to one plan document.
Status legend: `[ ]` not started · `[~]` scaffold exists, needs real work · `[x]` done

---

## 1. Master Catalog — Sys.Catalog Backing Store

**Plan**: `docs/plans/plan-master-catalog-and-metadata.md`
**Current state**: `internal/master/catalog/manager.go` uses `MemoryStore` (in-memory only). No `AlterTable`, no `DeleteTable`, no `TabletInfo` tracking, no BG reconcile loop, no async task dispatcher.

### 1a. `SysCatalogStore` — durable CatalogStore implementation

- [ ] Create package `internal/master/syscatalog/`.
- [ ] Define `SysCatalogStore struct` implementing `catalog.CatalogStore` interface (`Apply`, `LoadSnapshot`).
- [ ] Persistence model: use the existing RocksDB store (`internal/storage/rocks/store.go`) as the backing KV.
  - Key schema: `entity/<entity_type>/<entity_id>` → protobuf/JSON bytes.
  - Mutation log entry: `reqlog/<request_id>` → result hash for dedupe TTL.
- [ ] `Apply(ctx, CatalogMutation)` writes all `EntityOps` atomically using a RocksDB write batch.
- [ ] `LoadSnapshot(ctx)` scans all `entity/` keys and reconstructs `CatalogSnapshot` (tables, tableNameToID index).
- [ ] Expose `NewSysCatalogStore(rocksStore *rocks.Store) *SysCatalogStore`.
- [ ] Wire into `cmd/master/main.go`: replace `catalog.NewMemoryStore()` with `syscatalog.NewSysCatalogStore(...)`.
- [ ] Tests: write, restart, reload — verify snapshot is identical; test Apply idempotency via request ID.

### 1b. Extend `Manager` — AlterTable, DeleteTable, TabletInfo

- [ ] Add `TabletInfo` to `CatalogSnapshot` (map keyed by `TabletID`).
- [ ] Define `TabletState` enum in catalog matching plan: `Preparing → Creating → Running → Tombstoned → Deleted`.
- [ ] Add `CreateTablet(ctx, TabletInfo, reqID)` to `Manager`.
- [ ] Add `AlterTable(ctx, AlterTableRequest)` — transition `Running → Altering → Running`; bump `Version`; persist.
- [ ] Add `DeleteTable(ctx, DeleteTableRequest)` — transition to `Deleting → Deleted`; tombstone dependent tablets.
- [ ] Add `GetTableByName(ctx, namespaceID, name)` convenience lookup.
- [ ] `ProcessTabletReport(ctx, tsID, TabletReport)`:
  - Verify `SequenceNo` monotonicity per `(tsID, tabletID)`.
  - Drop stale/out-of-order updates (log them).
  - Apply valid deltas; emit `TabletAction` list (e.g., assign, remove, tombstone).
- [ ] All mutations: validate leader gate, check `ErrNotLeader` before touching state.
- [ ] All mutations: dedupe by `RequestID` → return prior result if seen.
- [ ] Tests: transition table invariant matrix; dedupe correctness; stale heartbeat filtering.

### 1c. BG Reconcile Loop

- [ ] Create `internal/master/bg/` package.
- [ ] `BGLoop struct` with `RunIteration(ctx) error` — called periodically (default 1 s per config).
- [ ] Each iteration:
  1. Snapshot current catalog state.
  2. Find tablets in `Preparing`/`Creating` with no assigned replicas → emit `CreateReplica` tasks.
  3. Find tablets in `Deleting` → emit `DeleteReplica` tasks.
  4. Find under-replicated tablets (replica count < RF) → emit `AddReplica` tasks.
  5. Respect `MaxConcurrentAdds` / `MaxConcurrentRemovals` caps from `CatalogConfig`.
- [ ] Tasks feed into `TaskDispatcher` (see 1d).
- [ ] Tests: deterministic scheduler — inject specific catalog states and assert emitted tasks.

### 1d. Async Task Dispatcher

- [ ] Create `internal/master/tasks/` package.
- [ ] `TaskDispatcher` holds a bounded worker pool (goroutines).
- [ ] Task types: `CreateReplicaTask`, `DeleteReplicaTask`, `MoveLeaderTask`.
- [ ] Each task: RPC to tserver (`CreateTablet` / `DeleteTablet`), update catalog on success/failure.
- [ ] On RPC failure: mark `ErrTransientDispatch`, BG loop retries next iteration.
- [ ] Per-tablet action serialization: only one in-flight task per tablet at a time.
- [ ] Tests: task dispatch with mock RPC; retry on transient failure; no double-dispatch per tablet.

---

## 2. Load Balancing and Replica Placement

**Plan**: `docs/plans/plan-load-balancing-and-replica-placement.md`
**Current state**: not started. No files exist.

### 2a. Data Structures and Package

- [ ] Create `internal/master/balancer/` package.
- [ ] Define structs as per plan:
  ```go
  type NodeLoad struct { NodeID, ReplicaCount, LeaderCount int, IsLive bool, Placement string }
  type TabletPlacement struct { TabletID string; Replicas []ReplicaPlacement; RF int }
  type BalanceAction struct { Type, TabletID, FromNode, ToNode, Reason string }
  type ClusterState struct { Nodes []NodeLoad; Tablets []TabletPlacement }
  type Violation struct { TabletID, Reason string }
  ```

### 2b. Planner — `PlanBalanceRound`

- [ ] Implement `PlanBalanceRound(state ClusterState) ([]BalanceAction, error)`:
  1. Compute per-node replica skew: `skew = count - mean`.
  2. **Priority 1** — under-replicated tablets (replica count < RF): emit `add_replica` on least-loaded live node satisfying placement constraint.
  3. **Priority 2** — over-replicated tablets (replica count > RF): emit `remove_replica` from most-loaded node.
  4. **Priority 3** — leader imbalance (if `LeaderBalancingEnabled`): emit `move_leader` from highest→lowest leader-count node.
  5. Cap emitted actions per round to `MaxConcurrentAdds + MaxConcurrentRemovals + MaxConcurrentLeaderMoves`.
  6. Skip tablets already in cooldown window (track last action timestamp in in-memory map).
- [ ] Placement constraint check: node `Placement` label must differ across replicas (rack/zone awareness). If insufficient distinct placements, log `Violation` and skip.

### 2c. Executor — `ExecuteBalanceAction`

- [ ] `ExecuteBalanceAction(action BalanceAction) error`: dispatch to `TaskDispatcher` (from 1d).
- [ ] On success: update catalog placement; reset cooldown timer for tablet.
- [ ] On failure: log, do not update cooldown (retry next round).

### 2d. Violation Reporting — `GetPlacementViolations`

- [ ] `GetPlacementViolations() []Violation`: scan all tablets for unsatisfied placement constraints; return list.
- [ ] Expose via admin HTTP endpoint `/varz` or dedicated `/placement-violations`.

### 2e. BG integration

- [ ] Invoke balancer from BG loop (1c) after assignment pass.
- [ ] Config: `RebalanceInterval` (default 1 s), `EnableLeaderBalancing` bool, per-action concurrency caps.

### 2f. Tests

- [ ] Skew correction: start with imbalanced state, run planner N rounds, assert convergence.
- [ ] Node failure: mark node dead, assert under-replicated tablets get repair actions.
- [ ] Blacklist drain: all tablets moved off blacklisted node within N rounds.
- [ ] No-oscillation: verify cooldown window prevents move → move back churn.

---

## 3. Snapshotting and Backup

**Plan**: `docs/plans/plan-snapshotting-and-backup.md`
**Current state**: not started. No files exist.

### 3a. Data Structures

- [ ] Create `internal/master/snapshot/` package.
- [ ] Define structs from plan:
  ```go
  type SnapshotState int  // Creating, Complete, Failed, Deleting, Restoring
  type SnapshotInfo struct { SnapshotID string; NamespaceIDs, TableIDs, TabletIDs []string; CreateHT uint64; State SnapshotState }
  type TabletSnapshotTask struct { SnapshotID, TabletID string; State SnapshotState; Error string }
  ```

### 3b. Coordinator State Machine

- [ ] `Coordinator struct` owns in-memory map of `SnapshotID → SnapshotInfo`.
- [ ] Per-snapshot mutex; coordinator worker pool with bounded concurrency (default 2, from config).
- [ ] `CreateSnapshot(req) (snapshotID, error)`:
  1. Validate namespace/table IDs exist in catalog.
  2. Freeze catalog version — store `CreateHT` (use hybrid clock).
  3. Persist `SnapshotInfo` with state `Creating` to sys.catalog.
  4. Fan out `TabletSnapshotTask` to each involved tablet via RPC.
  5. Aggregate responses: on all success → `Complete`; on any failure → `Failed`; attempt cleanup.
- [ ] `DeleteSnapshot(snapshotID)`: transition `Complete → Deleting`; fan out delete to tablets; mark done.
- [ ] `RestoreSnapshot(snapshotID, targetHT)`:
  1. Validate compatibility — catalog version must not have drifted in breaking way.
  2. Lock target table/tablet entities during restore.
  3. Fan out restore RPCs to tablets.
  4. On success: atomically publish restored metadata versions to catalog.
  5. On schema drift conflict: reject with `ErrPreconditionFailed` unless force policy set.
- [ ] `GetSnapshot(snapshotID) (SnapshotInfo, error)`.

### 3c. Tablet-side Snapshot RPC Handler

- [ ] Add `CreateTabletSnapshot(snapshotID, tabletID) error` RPC stub in tserver.
- [ ] Implementation: flush memtable, create RocksDB checkpoint into `<data-dir>/snapshots/<snapshot-id>/`.
- [ ] Add `DeleteTabletSnapshot(snapshotID, tabletID) error`: remove checkpoint directory.
- [ ] Add `RestoreTabletSnapshot(snapshotID, tabletID) error`: swap checkpoint into live paths atomically.

### 3d. Persistence

- [ ] Persist snapshot descriptors in sys.catalog under `entity/snapshot/<snapshot-id>`.
- [ ] Persist `TabletSnapshotTask` per `(snapshot-id, tablet-id)` for recovery.
- [ ] On coordinator restart: reload in-progress snapshots; re-fan-out incomplete tasks.

### 3e. Tests

- [ ] Create/restore end-to-end on single node.
- [ ] Partial tablet failure → snapshot marked `Failed`; cleanup attempted.
- [ ] Master failover during snapshot orchestration → recovery resumes.
- [ ] Schema drift detection: alter table between create and restore → assert rejection.

---

## 4. Tablet Lifecycle — Durable Markers and Full State Machine

**Plan**: `docs/plans/plan-tablet-lifecycle-and-management.md`
**Current state**: `internal/tablet/manager.go` has a basic registry but no on-disk lifecycle markers, no split algorithm, no full state machine (only basic `create`/`delete` stubs).
**Deferred in**: `tasks/deferred.md` (Phase 3).

### 4a. On-disk Lifecycle Markers

- [ ] On every state transition, write a marker file before updating in-memory state:
  - Path: `tablet-meta/<tablet-id>.meta` (full `TabletMeta` JSON/proto).
  - Tombstone: `tablet-meta/<tablet-id>.tombstone`.
- [ ] `TabletMeta` includes `State`, `StateVersion`, `SplitParentID`, `SplitDepth`, `RaftConfig`.
- [ ] Contract: **no in-memory state transition completes before the marker is durably written** (use `fsync` on the file).
- [ ] On manager startup: scan `tablet-meta/` directory, load all markers, reconstruct `peers` registry.

### 4b. Bootstrap Algorithm (from disk)

- [ ] `Bootstrapper.Bootstrap(tabletID string) (*TabletPeer, error)`:
  1. Load `TabletMeta` from `tablet-meta/<tablet-id>.meta`.
  2. Open regular DB (`data/<tablet-id>/regular`) and intents DB (`data/<tablet-id>/intents`).
  3. Read flushed frontier from RocksDB manifest.
  4. Replay WAL entries after frontier (`internal/wal/`).
  5. Validate: `last_applied_opid <= last_committed_opid`.
  6. Initialize `Consensus` peer as follower.
  7. Transition state to `Running`; update marker.
- [ ] WAL corruption: truncate to last valid op (CRC check each entry) and continue.
- [ ] On metadata mismatch (unexpected schema/version): mark `Failed`; require remote bootstrap.

### 4c. Full State Machine Enforcement

- [ ] Enforce all transitions from plan:
  ```
  NOT_STARTED → BOOTSTRAPPING → RUNNING | FAILED
  RUNNING → TOMBSTONED | SPLITTING | DELETING
  TOMBSTONED → REMOTE_BOOTSTRAPPING → RUNNING
  DELETING → DELETED
  FAILED → REMOTE_BOOTSTRAPPING | DELETING
  ```
- [ ] Per-tablet op token (`ops map[tabletID]*tabletOpToken`) serializes concurrent directives.
- [ ] Duplicate `CreateTablet` with equivalent metadata → idempotent success (`ErrAlreadyExistsEquivalent`).
- [ ] Directive version check: reject if directive `StateVersion` < current peer `StateVersion`.

### 4d. Split Algorithm

- [ ] `SplitTablet(ctx, tabletID, splitKey, reqID)`:
  1. Verify parent is `RUNNING` and split key is within partition bounds.
  2. Replicate a `SPLIT` op via parent consensus (WAL entry).
  3. Flush parent memtable; capture split frontier opID.
  4. Create two child `TabletMeta` entries with non-overlapping partitions (left: `[start, splitKey)`, right: `[splitKey, end)`).
  5. Write child markers (`NOT_STARTED`).
  6. Bootstrap children from parent checkpoint + WAL tail.
  7. Transition parent to `TOMBSTONED` (tombstone marker; retain for remote bootstrap).
  8. Report child tablets to master via heartbeat.
- [ ] Tests: split key boundary correctness; no key lost or double-counted after split.

### 4e. Delete / Tombstone Algorithm

- [ ] `DeleteTablet(ctx, tabletID, tombstone bool, reqID)`:
  1. Mark state `DELETING`; write marker.
  2. Stop peer services in order: RPC handlers → consensus → WAL → storage.
  3. If `tombstone=true`: retain `tablet-meta/<tablet-id>.tombstone`; remove data dirs.
  4. If `tombstone=false`: remove all dirs including meta.
  5. Mark `DELETED`; remove from registry.

### 4f. Tests

- [ ] State transition validation table unit test (all valid/invalid pairs).
- [ ] Crash during bootstrap replay: inject WAL corruption; verify truncation and recovery.
- [ ] Restart recovery from `DELETING`, `SPLITTING`, `REMOTE_BOOTSTRAPPING` intermediate states.
- [ ] High-parallel create/delete with master directives under race detector.

---

## 5. Remote Bootstrap and Recovery

**Plan**: `docs/plans/plan-remote-bootstrap-and-recovery.md`
**Current state**: `internal/tablet/remotebootstrap/session.go` — session struct exists, but transfer and install logic is stubbed.

### 5a. Source-side Bootstrap Service

- [ ] Add `BootstrapService` to tserver RPC server.
- [ ] `FetchManifest(sessionID) (BootstrapManifest, error)`:
  1. Freeze a RocksDB checkpoint at current flushed frontier.
  2. Collect file list from checkpoint dir + WAL segments after checkpoint frontier.
  3. Return `BootstrapManifest{TabletMeta, DBFiles []FileMeta, WALSegments []SegmentMeta, ConsensusMeta}`.
- [ ] `FetchFileChunk(sessionID, fileName, offset, size) (FileChunk, error)`:
  - Read chunk from checkpoint/WAL file; compute CRC32; return `FileChunk`.
  - Rate-limit to `BootstrapBandwidthCap` (default 256 MB/s) to avoid starving foreground I/O.
- [ ] `FinalizeBootstrap(sessionID)`: release checkpoint hold; clean up session.
- [ ] Session timeout: if no activity for `SessionTimeout` (default 10 m), auto-cleanup.
- [ ] Bound concurrent sessions per node to `MaxConcurrentBootstrapSessions` (default 4).

### 5b. Destination-side Bootstrap Client

- [ ] `RemoteBootstrapClient.Run(ctx, tabletID, sourcePeer) error`:
  1. Call `StartSession` on source → get `sessionID`.
  2. Call `FetchManifest` → get file list.
  3. For each file: stream chunks into `bootstrap_tmp/<tablet-id>/<session-id>/`; verify CRC32 per chunk; verify total file size.
  4. On chunk checksum mismatch: abort session, clean temp dir, retry with different source peer.
  5. After all files received: validate manifest completeness (all files present, sizes match).
  6. Atomic install: move temp dir contents into `data/<tablet-id>/`, `wals/<tablet-id>/`, `tablet-meta/<tablet-id>.meta`.
  7. Call `FinalizeBootstrap` on source.
  8. Start tablet from installed state (call `Bootstrapper.Bootstrap`).
  9. Resume consensus catch-up from frontier opID in manifest.

### 5c. Trigger from Consensus

- [ ] In `internal/raft/consensus.go`: when follower's `match_index` falls below log's GC boundary → set `needs_bootstrap=true` in `AppendEntriesResponse`.
- [ ] Tablet manager handles `needs_bootstrap`: transition state to `REMOTE_BOOTSTRAPPING`, invoke `RemoteBootstrapClient.Run`.

### 5d. Tests

- [ ] Interrupted transfer: kill source mid-stream; verify temp cleanup and retry with new session.
- [ ] Checksum corruption: flip a byte in a chunk; verify abort and source-switch.
- [ ] Post-bootstrap consistency: after bootstrap, apply 100 writes on leader, verify follower converges.
- [ ] Concurrent bootstrap sessions: `MaxConcurrentBootstrapSessions` cap enforced.

---

## 6. Distributed Transactions — Full Implementation

**Plan**: `docs/plans/plan-distributed-transaction-manager.md`
**Current state**: `internal/txn/manager.go` — coordinator FSM and apply loop work, but all in one package. Missing: separate participant package, conflict resolver, wait queue, deadlock detector, persistence.

### 6a. Refactor into Sub-packages

- [ ] Create `internal/txn/coordinator/` — move and extend `Manager`.
- [ ] Create `internal/txn/participant/` — per-tablet participant apply state.
- [ ] Create `internal/txn/conflict/` — conflict resolver.
- [ ] Create `internal/txn/waitq/` — wait queue and deadlock detector.
- [ ] Create `internal/txn/statuscache/` — local txn status cache for participant reads.

### 6b. Coordinator — Persistence

- [ ] Status records are stored in the **status tablet** (a special tablet designated per transaction range).
- [ ] `TxnRecord` persisted via the tablet's DocDB engine (key: `txn/<txn_id>`, value: serialized `TxnRecord`).
- [ ] On coordinator startup: scan status tablet for records in non-terminal states (`COMMITTING`, `ABORTING`) → resume apply/cleanup.
- [ ] `ParticipantApplyState` persisted per `(txn_id, tablet_id)` for chunked apply resume.

### 6c. Coordinator — Missing `TxnMeta` Fields

- [ ] Add `Priority`, `StartHT`, `Isolation`, `StatusTablet`, `InvolvedTablets` to `Record` (matching plan's `TxnRecord`).
- [ ] `Begin`: store `InvolvedTablets` as they are declared; update on each new participant.
- [ ] `Commit`: CAS `PENDING → COMMITTING`; persist `COMMITTED` with `commit_ht`; dispatch async apply tasks to all `InvolvedTablets`.
- [ ] Return success immediately after status tablet write (`COMMITTED`) — participant apply is async.

### 6d. Participant Package

- [ ] `Participant` struct holds `ApplyState map[TxnID]ParticipantApplyState`.
- [ ] `ApplyIntents(ctx, txnID, commitHT, limit)`:
  - Read up to `limit` intent records from intents DB for `txnID`.
  - Write resolved (committed) values to regular DB.
  - Delete intent records.
  - Persist `LastAppliedKey` for resumable chunking.
  - Return `done=true` when all intents applied.
- [ ] `RemoveIntents(ctx, txnID, limit)`: delete intent records for aborted txn (chunked).

### 6e. Conflict Resolver

- [ ] `ConflictResolver.Check(ctx, keys [][]byte, myTxnID TxnID, myPriority uint64) error`:
  1. Scan intents DB for each key.
  2. For each conflicting intent: look up the txn status (via `statuscache` → coordinator RPC if cache miss).
  3. `ABORTED`: safe to ignore.
  4. `COMMITTED`: compare `commit_ht` with current reader's snapshot HT → if `commit_ht > snapshot_ht`, return `ErrTxnRestartRequired`.
  5. `PENDING`: if wait queues disabled → priority-based: abort lower-priority txn; else enqueue in wait queue.

### 6f. Wait Queue and Deadlock Detector

- [ ] `WaitQueue` struct: `waiters map[TxnID][]TxnID` (waiter → list of blocking txns).
- [ ] `Enqueue(waiter, blocker TxnID)`: add to graph; signal waiter when blocker resolves.
- [ ] `DeadlockDetector` goroutine (interval: `DeadlockProbeInterval` default 1 s):
  1. Build wait-for graph from `WaitQueue`.
  2. Detect cycles (DFS with coloring).
  3. Pick victim: lowest `Priority`; on tie, lowest `TxnID` lexicographically.
  4. Abort victim via coordinator.
- [ ] `DeadlockDetector` uses a separate lock from `WaitQueue` enqueue — never block enqueue on cycle detection.

### 6g. Tests

- [ ] FSM transition table: all valid/invalid state pairs.
- [ ] Idempotent commit/abort: same `reqID` twice → same result.
- [ ] Conflict decision matrix: all combinations of `(ABORTED, COMMITTED, PENDING)` conflicting intents.
- [ ] Multi-tablet commit + abort: two tablets, commit path end-to-end.
- [ ] Coordinator failover during `COMMITTING`: reload from persisted status record, resume apply.
- [ ] Induced deadlock cycle: two txns cross-lock two keys → detector aborts exactly one; other commits.
- [ ] Chunked apply resume: simulate crash after partial `ApplyIntents`; restart and verify all intents applied.

---

## 7. Raft Consensus — Missing Features

**Plan**: `docs/plans/plan-raft-consensus.md`
**Current state**: `internal/raft/consensus.go` — leader election, append, commit index, metadata store. Missing: pre-vote, dynamic membership change, leader lease reads, `needs_bootstrap` signal, WAL GC integration.

### 7a. Pre-vote (Election Optimization)

- [ ] Add `PreVote` RPC: candidate sends pre-vote with current term+1 before incrementing term.
- [ ] Receiver grants pre-vote only if: its own leader lease has expired AND candidate log is at least as up-to-date.
- [ ] Candidate promotes to real vote phase only after majority pre-votes received.
- [ ] Prevents term inflation from isolated nodes.

### 7b. Leader Lease Reads

- [ ] `GetLeaderLeaseStatus() (valid bool, expiresAt time.Time)`.
- [ ] Leader refreshes `LeaderLeaseUntil` on every quorum-acked heartbeat: `now + LeaderLeaseDuration`.
- [ ] `ReadAtLease(ctx)`: succeeds without round-trip if `now < LeaderLeaseUntil`; else redirect to leader.
- [ ] Config: `LeaderLeaseDuration` default 2 s.

### 7c. Dynamic Membership Change

- [ ] `ChangeConfig(ctx, op ConfigChangeOp) error` — `op` is `AddPeer` or `RemovePeer`.
- [ ] One config change in flight at a time: gate on `ConfigInProgress` flag.
- [ ] Replicate `CONFIG_CHANGE` entry via normal log.
- [ ] Apply config only after commit (joint consensus not required for single-peer changes).
- [ ] Persist new `RaftConfig` in `ConsensusMetadata` via `ConsensusMetaStore`.
- [ ] Tests: add a 4th peer to 3-node group; remove a peer; verify quorum still works.

### 7d. Remote Bootstrap Trigger

- [ ] In `AppendEntriesResponse`: add `NeedsBootstrap bool` field.
- [ ] Leader: if follower's `last_received_opid` is before log's GC boundary → set `NeedsBootstrap=true` in response.
- [ ] Tablet manager listener: on `NeedsBootstrap`, call `RemoteBootstrapClient.Run` (from section 5).

### 7e. WAL GC Coordination

- [ ] Leader tracks per-peer `MatchIndex`; WAL anchor = `min(MatchIndex)` across all peers.
- [ ] WAL GC is only allowed below the anchor to prevent bootstrap-required scenarios when not needed.

### 7f. Tests

- [ ] Pre-vote: isolated node with stale term does not disrupt cluster after reconnect.
- [ ] Lease read: `GetLeaderLeaseStatus` returns valid within 2 s of last heartbeat.
- [ ] Membership change: add/remove peer, verify quorum acks still require majority of new config.
- [ ] WAL GC: with lagging peer, GC does not truncate past peer's `MatchIndex`.

---

## 8. YSQL Query Layer

**Plan**: `docs/plans/plan-ysql-query-layer.md`
**Current state**: `internal/query/sql/coordinator.go` — `LocalCoordinator` manages config/lifecycle state only. No PostgreSQL process, no actual query execution.

### 8a. PostgreSQL Process Management

- [ ] `LocalCoordinator.Start()`: when `cfg.Enabled`:
  1. Validate data dir exists or `initdb` to create it.
  2. Generate `postgresql.conf` from `ExtraConf` + defaults.
  3. Generate `pg_hba.conf` from `HBAConfig`.
  4. Launch `postgres` (or `postmaster`) subprocess with correct data dir and bind address.
  5. Wait for postmaster to accept connections (poll Unix socket / TCP with timeout).
  6. Supervise process: if it dies, mark unhealthy and attempt restart up to N times.
- [ ] `LocalCoordinator.Stop()`: send `SIGTERM` to postmaster; wait for clean shutdown; kill after timeout.
- [ ] `LocalCoordinator.Health()`: ping postmaster via `pg_isready` or trivial TCP connect.
- [ ] Add `GetProcess() *os.Process` for test introspection.

### 8b. PgClient Service RPC

- [ ] Define `PgClientService` RPC methods in `internal/rpc/`:
  - `Perform(PgClientRequest) (PgClientResponse, error)` — multiplex reads/writes/DDL.
- [ ] `PgClientRequest` carries `PgReadOp` or `PgWriteOp` (from `plan-pggate-bridge.md` data structures).
- [ ] `PgClientService` handler: routes to tablet via partition map → dispatch to correct `TabletPeer`.

### 8c. Schema Cache Coherence

- [ ] Coordinator tracks `CatalogVersion uint64`.
- [ ] On each query: if backend's `CatalogVersion` != master's current version → invalidate PostgreSQL schema cache (`pg_catalog` invalidation signal) and retry.
- [ ] Expose `GetCatalogVersion()` RPC on master for polling.

### 8d. Tests

- [ ] Process start/stop lifecycle (no real PG process needed in unit test — use fake subprocess).
- [ ] SQL conformance suite: basic `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE` via `pgx` driver.
- [ ] Failover test: kill tserver during transaction → client gets retriable error; reconnect and commit.
- [ ] Schema change race: `ALTER TABLE` mid-query → cache invalidation and retry fires.

---

## 9. YCQL Query Layer

**Plan**: `docs/plans/plan-ycql-query-layer.md`
**Current state**: `internal/query/cql/server.go` — `LocalServer` manages connections and prepared-statement cache. `Route()` returns `Applied: true` without executing anything. No CQL wire protocol.

### 9a. CQL Binary Protocol Listener

- [ ] Add TCP listener in `LocalServer.Start()` on `cfg.BindAddress`.
- [ ] Per-connection: spawn goroutine running `CQLProcessor`.
- [ ] `CQLProcessor` reads CQL native protocol v4 frames (header: 9 bytes — version, flags, stream, opcode, length).
- [ ] Implement opcodes: `STARTUP`, `OPTIONS`, `QUERY`, `PREPARE`, `EXECUTE`, `BATCH`, `REGISTER`, `AUTH_RESPONSE`.
- [ ] Respond with proper frame types: `READY`, `RESULT`, `PREPARED`, `ERROR`, `EVENT`.
- [ ] Use `encoding/binary` (big-endian); implement CQL type codec for `int`, `bigint`, `text`, `uuid`, `timestamp`, `boolean`, `blob`.

### 9b. CQL Parser and Analyzer

- [ ] Minimal CQL parser (hand-written or use a Go CQL parser library) supporting:
  - `SELECT`, `INSERT`, `UPDATE`, `DELETE` DML.
  - `CREATE TABLE`, `DROP TABLE`, `CREATE KEYSPACE` DDL.
  - `USING TTL`, `IF NOT EXISTS`, `IF EXISTS` modifiers.
  - Partition key and clustering column expressions in `WHERE`.
- [ ] Analyzer: validate table/keyspace existence against catalog; resolve column types; bind `?` placeholders to types.
- [ ] Schema version check per statement execution: if `PreparedStmt.SchemaVer != current` → return `UNPREPARED` error → client re-prepares.

### 9c. Execution — Route to Tablet

- [ ] For each DML: extract partition key → compute token → route to tablet leader.
- [ ] Multi-partition reads (`IN`, range scans): fan out to multiple tablets; merge and sort results.
- [ ] Conditional writes (`IF` clause): execute read-modify-write within a single-row transaction via `TxnManager`.
- [ ] Batch (`BATCH`): wrap all statements in a single `TxnManager.Begin/Commit` for `LOGGED` batch; no transaction for `UNLOGGED`.

### 9d. Result Encoding

- [ ] Encode `ROWS` result: `result_metadata` (column count, flags, paging state), then row bytes per CQL type codec.
- [ ] Encode `VOID` result for mutations.
- [ ] Paging: if `FetchSize` exceeded, embed `paging_state` opaque bytes for next page fetch.

### 9e. Tests

- [ ] Protocol compliance: connect via `gocql` driver; smoke `CREATE KEYSPACE`, `CREATE TABLE`, `INSERT`, `SELECT`.
- [ ] Prepared invalidation: alter table schema; subsequent execute returns `UNPREPARED`; client re-prepares; retry succeeds.
- [ ] Batch semantics: `LOGGED` batch — all-or-nothing; `UNLOGGED` batch — independent.
- [ ] TTL: insert with `USING TTL 1`; wait 1 s; verify row not returned.
- [ ] Paging: insert 1000 rows; page through with `FetchSize=100`; verify all 1000 received exactly once.

---

## 10. PgGate Bridge — Real Storage Dispatch

**Plan**: `docs/plans/plan-pggate-bridge.md`
**Current state**: `internal/query/pggate/executor.go` — `ExecRead` returns fake row count; `ExecWrite` queues ops but only the flush path dispatches them (and flush also is in-memory only). No actual tablet I/O.

### 10a. Write Dispatch

- [ ] `FlushWrites(ctx, sessionID)`:
  1. Take all `PendingWrites` from session buffer.
  2. Group by `TableID` → resolve tablet ID via partition map.
  3. For each tablet group: build write batch; dispatch `TabletWrite` RPC to tablet leader.
  4. On `ErrNotLeader`: refresh tablet leader cache; retry up to `retryCap` times.
  5. On conflict: increment `WriteRestartConflicts`; return `ErrConflict` for client retry.
  6. On success: clear buffer; return list of applied `WriteOp`.

### 10b. Read Dispatch

- [ ] `ExecRead(ctx, sessionID, op ReadOp)`:
  1. Resolve `op.TableID` + `op.BindVars` (partition key values) → tablet ID.
  2. Send `TabletRead` RPC with `op.Targets` (projected columns), `op.FetchSize`, and snapshot HT from session txn.
  3. On `ErrNotLeader`: redirect; on `ErrTxnRestartRequired`: return `ErrConflict`.
  4. Return actual row count from tablet response.
- [ ] For index scans: send `op.IndexID` in read request; tablet resolves secondary index → primary key → row.

### 10c. Transaction Integration

- [ ] `BeginTxn(ctx, sessionID, isolation)`: call `TxnCoordinator.Begin`; store `TxnHandle` in session with `TxnID` and `StartHT`.
- [ ] `CommitTxn(ctx, sessionID, reqID)`: flush remaining writes; call `TxnCoordinator.Commit`; clear session txn.
- [ ] `AbortTxn(ctx, sessionID, reqID)`: call `TxnCoordinator.Abort`; clear session buffer.
- [ ] Savepoints: record buffer watermark at savepoint; on rollback, truncate buffer to watermark and issue `TxnCoordinator` partial rollback.

### 10d. Catalog Version Cache

- [ ] Session holds `CatalogVersion uint64`.
- [ ] Before each operation: compare with master's current version (poll or push notification).
- [ ] On mismatch: invalidate `TableCache`; reload table descriptors; retry operation.

### 10e. Tests

- [ ] `ExecWrite` with flush: verify data appears in tablet storage.
- [ ] `ExecRead` after write: read back written value; confirm row count and values.
- [ ] `ON CONFLICT` / upsert: write same key twice; verify idempotent result.
- [ ] Savepoint rollback: write 3 rows, savepoint, write 2 more, rollback to savepoint, commit — only 3 rows visible.
- [ ] Catalog version mismatch: alter schema mid-session; verify cache invalidation and retry.

---

## 11. CDC / xCluster — Real Network Transport

**Plan**: `docs/plans/plan-cdc-and-xcluster-replication.md`
**Current state**: `internal/replication/cdc/` and `internal/replication/xcluster/` — in-memory event store, file-backed checkpoints, apply loop, idempotent dedup all work. Missing: real network (multi-node producer/consumer RPC), CDCSDK virtual WAL, tablet split stream remapping.

### 11a. Real CDC Producer — WAL/Intent Read

- [ ] `Producer struct` owns a `WAL` reference and `DocDBEngine` intents reference.
- [ ] `GetChanges(req GetChangesRequest) (GetChangesResponse, error)`:
  1. Read WAL entries from `req.FromOpID` forward.
  2. For each committed-transaction entry: resolve intent values from intents DB → build `CDCRecord` with `CommitHT`, `Key`, `Value`, `OpType`.
  3. Filter to only records belonging to `req.StreamID` (namespace/table filter).
  4. Return batch up to `MaxBatchBytes`.
  5. If no new records: adaptively back off (idle delay 100 ms, with multiplier up to max).
- [ ] `SetCheckpoint(req)`: persist `StreamCheckpoint` to `cdc_state` system table keyed by `(stream_id, tablet_id)`.

### 11b. Multi-node xCluster Poller

- [ ] `Poller struct` connects to source cluster via RPC client (`internal/rpc/client.go`).
- [ ] Per `(stream_id, tablet_id)` goroutine:
  1. Load last `StreamCheckpoint` from local `cdc_state`.
  2. Call source `GetChanges` RPC with `FromOpID = checkpoint.{Term, Index}`.
  3. On response: apply each `CDCRecord` to local target tablet (write through `DocDBEngine`).
  4. On successful apply: advance checkpoint; persist.
  5. On source unavailable: backoff retry (do not advance checkpoint).
  6. On WAL retention breach (`ErrWALRetentionExceeded`): trigger full re-bootstrap of the stream.
- [ ] Bounded apply worker pool on target cluster (configurable concurrency).

### 11c. Tablet Split Stream Remapping

- [ ] When parent tablet is split: parent `(stream_id, parent_tablet_id)` checkpoint is mapped to both children.
- [ ] `SplitRemapper`: on detecting parent tombstone, create two child `StreamCheckpoint` records starting from parent's last checkpoint.
- [ ] Resume polling child tablets independently.

### 11d. CDCSDK Virtual WAL (External Consumer API)

- [ ] `VirtualWAL struct`: presents an ordered, deduplicated stream of change records to external consumers.
- [ ] `Subscribe(streamID, fromCheckpoint) (<-chan CDCRecord, error)`: returns Go channel receiving records in commit-timestamp order.
- [ ] Internally: fan-in from multiple tablet pollers; order by `CommitHT`; deduplicate by `OpID`.
- [ ] Expose via HTTP SSE or gRPC streaming endpoint on tserver admin port.

### 11e. Stream Metadata

- [ ] `CreateStream(req)`: persist stream metadata in master catalog; assign tablet subscriptions.
- [ ] `DeleteStream(streamID)`: stop pollers; delete `cdc_state` records; release WAL retention hold.
- [ ] WAL retention anchor: while any active stream references a tablet, WAL GC cannot go below that stream's checkpoint.

### 11f. Observability

- [ ] Per-stream metrics: `cdc_replication_lag_ms{stream_id, tablet_id}`, `cdc_records_applied_total`, `cdc_apply_errors_total`, `cdc_checkpoint_staleness_ms`.

### 11g. Tests

- [ ] Cross-node end-to-end: write to source cluster; verify records appear on target cluster within SLO.
- [ ] Source restart mid-stream: poller reconnects; resumes from last checkpoint; no records lost or double-applied.
- [ ] Split handling: split parent tablet; verify both child streams continue without gap.
- [ ] WAL retention breach: advance GC past stream checkpoint; verify re-bootstrap triggered.
- [ ] `DeleteStream`: after delete, verify WAL retention is released and GC can proceed.

---

## 12. Observability — pprof, Structured Logging, Tracing, Metrics

**Plan**: `docs/plans/plan-observability-and-operations.md`
**Current state**: `internal/replication/observability/` — `Registry` with counters, `MetricsHandler`, `VarzHandler`, `RPCzHandler` work. `PProfHandler` returns `"pprof placeholder\n"`.

### 12a. Real pprof Integration

- [ ] In `PProfHandler()`: import `net/http/pprof`; register standard pprof routes:
  ```go
  mux.HandleFunc("/debug/pprof/", pprof.Index)
  mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
  mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
  mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
  mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
  ```
- [ ] Remove the `"pprof placeholder\n"` body from `internal/replication/observability/admin.go:72`.
- [ ] Tests: HTTP GET `/debug/pprof/` returns index page; `/debug/pprof/goroutine` returns valid output.

### 12b. Structured Logging

- [ ] Add `LogContext` struct to `Registry`:
  ```go
  type LogContext struct { NodeID, TabletID, TraceID, RequestID string }
  ```
- [ ] Replace ad-hoc `log.Printf` calls across packages with a structured logger (use `log/slog` from stdlib, Go 1.21+).
- [ ] Each log line emits `node_id`, `tablet_id`, `trace_id`, `request_id` as structured fields.
- [ ] `Registry.SetLogContext(lc LogContext)` — used by server startup to inject node identity.
- [ ] Async log writer: buffer log records in a bounded channel; flush goroutine writes to `os.Stderr` or file. On channel full: drop with counter increment (never block caller).
- [ ] Config: `LogLevel` (debug/info/warn/error), `LogFile` (stdout default), `LogRotateMaxBytes`.

### 12c. Prometheus-compatible Metrics

- [ ] Extend `Registry` to support typed metric descriptors (`MetricDescriptor{Name, Type, Help, Labels}`).
- [ ] Types: `counter`, `gauge`, `histogram`.
- [ ] `RecordMetric(name, value, labels...)`: thread-safe update.
- [ ] Histogram: pre-defined bucket boundaries; track per-bucket counts + sum.
- [ ] `MetricsHandler()`: emit in Prometheus text format:
  ```
  # HELP raft_append_latency_ms Append latency histogram
  # TYPE raft_append_latency_ms histogram
  raft_append_latency_ms_bucket{le="1"} 42
  ...
  raft_append_latency_ms_sum 1234
  raft_append_latency_ms_count 50
  ```
- [ ] Register subsystem metrics at startup: Raft, WAL, DocDB, CDC, txn, catalog — all defined in plan §11.

### 12d. Distributed Tracing (OpenTelemetry)

- [ ] Add `go.opentelemetry.io/otel` dependency.
- [ ] Initialize `TracerProvider` in `ServerRuntime.Init()` with configurable exporter (stdout/OTLP).
- [ ] Propagate `TraceID` in RPC request headers (`internal/rpc/codec.go`).
- [ ] In each RPC handler: extract span from context; create child span; add semantic attributes (`rpc.method`, `tablet_id`).
- [ ] Sampling: `TraceSamplePercent` config (default 1%); use `sdktrace.ParentBased(sdktrace.TraceIDRatioBased(...))`.
- [ ] Tests: tracing propagation test — verify `TraceID` is same across RPC boundary.

### 12e. Operational Controls

- [ ] `/api/compact?tablet_id=<id>` HTTP endpoint: trigger manual RocksDB compaction on tablet.
- [ ] `/api/drain` HTTP endpoint: set node to drain mode (stop accepting new RPCs; wait for in-flight to complete); used for graceful shutdown.
- [ ] `/api/tablets` HTTP endpoint: list all `TabletPeer` states, WAL tail, consensus role.
- [ ] `/api/cancel?request_id=<id>` HTTP endpoint: cancel a live query (set cancellation flag in request context).

---

## 13. Server Lifecycle — Config, Memory Tracker, Graceful Shutdown

**Plan**: `docs/plans/plan-server-lifecycle-and-config.md`
**Current state**: `internal/server/runtime.go` — basic start/stop. Config parsing works. Missing: memory tracker, graceful drain, fs manager layout, config validation depth.

### 13a. Filesystem Layout and `FSManager`

- [ ] `FSManager struct` in `internal/platform/`:
  - `DataDirs []string`, `WALDirs []string`.
  - `Init()`: create directory tree for each dir: `data/`, `wals/`, `tablet-meta/`, `consensus-meta/`, `snapshots/`.
  - `TabletDataDir(tabletID)`, `TabletWALDir(tabletID)`, `TabletMetaPath(tabletID)` — canonical path helpers.
- [ ] `ServerBaseConfig.DataDirs` validated on init: all dirs must be writable; fail-fast if not.
- [ ] `NodeInstanceFile`: write `node-instance.pb` (node UUID + generation) to each data dir on first startup; verify on subsequent startups (detect data dir swap).

### 13b. Memory Tracker

- [ ] `MemTracker struct` in `internal/platform/`:
  - `HardLimitBytes int64`.
  - `Consume(bytes int64) error` — returns `ErrOOMKill` if over limit; caller must propagate.
  - `Release(bytes int64)`.
  - `CurrentUsage() int64`.
- [ ] Register per-subsystem child trackers: `raft-log`, `docdb-memtable`, `cdc-event-store`, `pggate-buffer`.
- [ ] Periodic reporter goroutine: log `memory_usage_bytes` gauge every 30 s.
- [ ] On `HardLimitBytes` exceeded: trigger `TerminationMonitor.TriggerOOM()` — initiate coordinated process exit.

### 13c. Graceful Shutdown Ordering

- [ ] `ServerRuntime.Stop(ctx)` must stop services in reverse dependency order:
  1. Close RPC listener (stop accepting new connections).
  2. Drain in-flight RPC handlers (wait with timeout, default 30 s).
  3. Stop query coordinators (YSQL/YCQL).
  4. Stop `BGLoop` and `TaskDispatcher`.
  5. Stop all `TabletPeer` instances (stop consensus → flush WAL → close storage).
  6. Stop CDC pollers.
  7. Close RocksDB instances.
  8. Release `FSManager` locks.
- [ ] Each step: log phase entry/exit; set `shutdownPhase` counter for metrics.
- [ ] Implement `drain` HTTP endpoint (see 12e) to pre-stop accepting requests before full shutdown.

### 13d. Config Validation

- [ ] On `Init()`: validate all `ServerBaseConfig` fields:
  - `NodeID` not empty.
  - `RPCBindAddress` parseable as `host:port`.
  - `DataDirs` and `WALDirs` not empty, not overlapping.
  - `MemoryHardLimitBytes > 0`.
  - `MaxClockSkew` in `[1ms, 10s]` range.
- [ ] Return `ErrInvalidConfig` with field name and reason on any validation failure.

### 13e. Tests

- [ ] Config validation: each invalid field produces correct `ErrInvalidConfig`.
- [ ] FS layout: after `Init()`, verify all expected subdirectories exist.
- [ ] Memory tracker: consume to limit; verify `ErrOOMKill` returned.
- [ ] Graceful shutdown: bring up mini-cluster; issue shutdown; verify all goroutines exit within timeout (use `goleak`).

---

## 14. Testing Infrastructure — Fault Injection and Test Cluster

**Plan**: `docs/plans/plan-testing-strategy.md`
**Current state**: `scripts/` has quick/standard test tiers, stress scenarios S1–S4, compose harness. Missing: `TestCluster` abstraction, `InjectFault` API, linearizability checker, network partition injection.

### 14a. `TestCluster` Abstraction

- [ ] Create `internal/testing/cluster/` package.
- [ ] `StartTestCluster(spec TestClusterSpec) (*ClusterHandle, error)`:
  - Starts `NumMasters` master processes and `NumTServers` tserver processes in-process (using `internal/server/runtime.go`) with ephemeral ports and temp directories.
  - Returns `ClusterHandle` with per-node handles.
- [ ] `ClusterHandle.Master(i int) *MasterHandle` — access master RPC, HTTP, admin.
- [ ] `ClusterHandle.TServer(i int) *TServerHandle` — access tserver.
- [ ] `ClusterHandle.Teardown()` — stop all nodes; clean temp dirs.
- [ ] `TestClusterSpec`: `NumMasters`, `NumTServers`, `ReplicationFactor`, `EnableYSQL`, `EnableYCQL`.

### 14b. `FaultInjector` — Network Partition and Kill

- [ ] `FaultInjector struct` wraps the cluster's inter-node RPC transport.
- [ ] `InjectFault(action FaultAction) error`:
  - `partition`: drop all RPC between `FromNode` and `ToNode` (intercept at `internal/rpc/client.go`).
  - `kill`: call `runtime.Stop()` on target node.
  - `delay`: inject artificial latency (sleep in RPC send path).
  - `diskfull`: make FS writes return errors (via `FSManager` error injection hook).
  - `clockskew`: offset `HybridClock` on target node by `Value` duration.
- [ ] `HealFault(action FaultAction)`: remove previously injected fault.
- [ ] Each fault identified by unique `FaultID`; `HealAll()` clears all active faults.

### 14c. Workload Runner

- [ ] `RunWorkload(spec WorkloadSpec) (WorkloadResult, error)`:
  - Spec: `RPS int`, `DurationSeconds int`, `ReadFraction float64`, `TableID string`.
  - Workers: goroutine pool issuing reads/writes against the cluster's CQL or SQL API.
  - Collects: `TotalOps`, `Errors`, `P50/P95/P99LatencyMs`, `ThroughputOPS`.
- [ ] `WorkloadResult` returned; compare against SLO thresholds in test assertions.

### 14d. Invariant Checkers

- [ ] `AssertInvariant(name string) error`:
  - `"no_lost_writes"`: for each write acked by cluster, verify the key is readable.
  - `"replica_convergence"`: all replicas of each tablet have same last-applied opID within 5 s.
  - `"no_uncommitted_intents"`: after workload stops, no non-terminal txn intents remain.
  - `"no_double_apply"`: verify xCluster apply dedup — no record applied twice on target.
- [ ] Implement using `ClusterHandle` admin APIs (tablet introspection endpoint from 12e).

### 14e. Scenarios Required by Plan

- [ ] **Consensus failover suite**: 3-node Raft group; kill leader; assert new leader elected < 6 s; no write lost.
- [ ] **Transaction anomaly suite**: concurrent write workload; assert no dirty reads, no lost updates.
- [ ] **Replication resume suite**: pause xCluster poller; write 10k rows on source; resume; assert all rows on target.
- [ ] **Partition + heal**: isolate leader from majority; assert cluster elects new leader; heal; assert old leader steps down.
- [ ] **P95 latency regression**: baseline run; assert p95 write latency < 50 ms for simple workload.

### 14f. CI Integration

- [ ] Add `scripts/test-integration.ps1`: run `TestCluster`-based integration tests (tag `//go:build integration`).
- [ ] Add `scripts/test-exhaustive.ps1`: run fault injection scenarios (tag `//go:build exhaustive`).
- [ ] CI tiers: quick (< 30 s), standard (< 5 min), integration (< 15 min), exhaustive (< 60 min).
- [ ] Capture test artifacts on failure: logs from all nodes, metrics snapshot, fault timeline.

---

---

## Implementation Log

| # | Section | Status | Notes |
|---|---|---|---|
| §4 | Tablet lifecycle durable markers + recovery | ✅ Done | `meta_store.go` + `NewManagerWithFS`; 10 new FS tests; all 21 tablet tests pass |
| §7 | Raft: pre-vote, leader lease reads, `NeedsBootstrap`, `ChangeConfig` | ✅ Done | `HandlePreVote`, `GetLeaderLeaseStatus`, `ReadAtLease`, `ChangeConfig`, `Peers`; 18 new tests; also added `ErrLeaseExpired`/`ErrBootstrapRequired`/`ErrInvalidConfig` to errors |
| §6 | Distributed txn: wait queue, deadlock detector, conflict resolver, TxnMeta fields | ✅ Done | `internal/txn/waitq/` (queue+detector, 15 tests), `internal/txn/conflict/` (resolver, 13 tests), `Record` extended with Priority/StartHT/Isolation/StatusTablet/InvolvedTablets, `RegisterTablet` added, `ErrTxnRestartRequired` added |
| §5 | Remote bootstrap: client, session cap, timeout, abort, CRC validation | ✅ Done | `client.go` (Client+Source+Installer), `ManagerConfig` with cap/timeout/NowFn, `FinalizeBootstrap`/`FinalizeWithValidation` split, `AbortSession`, `ExpireStaleSessions`, `ActiveCount`; 11 tests |
| §1 | Master catalog: SysCatalogStore (RocksDB-backed), AlterTable, DeleteTable, GetTableByName, TabletInfo, CreateTablet, ProcessTabletReport | ✅ Done | `internal/master/syscatalog/` (5 tests); Manager extended with 6 new methods + TabletInfo/TabletState; 13 new catalog tests |
| §2 | Load balancer: PlanBalanceRound (3 priority levels), GetPlacementViolations, rack-aware placement, cooldown, concurrency caps | ✅ Done | `internal/master/balancer/` (13 tests); NodeLoad, TabletPlacement, BalanceAction, ClusterState, Violation |
| §3 | Snapshotting: Coordinator (CreateSnapshot/DeleteSnapshot/RestoreSnapshot/GetSnapshot/ListSnapshots), TabletSnapshotRPC interface, bounded concurrent fan-out | ✅ Done | `internal/master/snapshot/` (14 tests); State machine with CREATING/COMPLETE/FAILED/DELETING/RESTORING |
| §10 | PgGate real dispatch: TabletDispatcher, PartitionResolver, TxnCoordinator interfaces; FlushWrites/ExecRead/BeginTxn/CommitTxn/AbortTxn wired to real dispatch | ✅ Done | `dispatch.go`; SnapshotHT in TxnHandle; BindVars in WriteOp; 12 dispatch tests; all existing tests pass |
| §12 | Observability: real pprof (PProfMux), slog structured logging (LogContext), histograms (RegisterHistogram/ObserveHistogram), Prometheus text format, drain/tablets/compact/cancel endpoints | ✅ Done | 17 new observability tests; Registry extended with draining flag, histograms map, slog logger |
| §13 | Server lifecycle: FSManager, MemTracker, ValidateConfig, graceful shutdown phases | ✅ Done | `internal/platform/fsmanager.go`, `internal/platform/memtracker.go`; `ErrOOMKill` added; `shutdownPhase` counter in Runtime |
| §14 | Testing infrastructure: TestCluster, FaultInjector, WorkloadRunner, invariant checkers | ✅ Done | `internal/testing/cluster/`, `internal/testing/fault/`, `internal/testing/workload/`, `internal/testing/invariant/` |
| §11 | CDC network transport, split remapper, metrics registry | ✅ Done | `internal/replication/cdc/net_producer.go`, `split_remapper.go`, `metrics.go`; `RPCProducer`, `Poller`, `SplitRemapper`, `MetricsRegistry` |
| §8 | YSQL PostgreSQL process management | ✅ Done | `internal/query/sql/pgprocess.go`; `PGProcess`, `InitDB`, `Start`, `Stop`, `Health`, `CatalogVersion` |
| §9 | YCQL binary protocol | ✅ Done | `internal/query/cql/protocol.go`, `messages.go`, `listener.go`; CQL frames, codec, request/response types, TCP listener |

---

## Priority Order for First Pass

The roadmap (`docs/plans/implementation-phases-roadmap.md`) defines dependency gates. Recommended order:

1. **§4** Tablet lifecycle durable markers — unblocks §5 (remote bootstrap) and §7 (Raft GC).
2. **§7** Raft missing features — pre-vote and lease reads are low-risk and high-value.
3. **§5** Remote bootstrap — needed for production cluster resilience.
4. **§6** Distributed txn refactor + wait queue + deadlock — unblocks §10 (PgGate real dispatch).
5. **§1** Catalog sys.catalog durable store + AlterTable/DeleteTable — unblocks §2 and §3.
6. **§2** Load balancer — depends on §1.
7. **§3** Snapshotting — depends on §1.
8. **§10** PgGate real dispatch — depends on §6.
9. **§8** YSQL process management — depends on §10.
10. **§9** YCQL binary protocol — depends on §6.
11. **§11** CDC real network transport — depends on §1, §4.
12. **§12** Observability (pprof, tracing, structured logging) — can be parallelized with any above.
13. **§13** Server lifecycle hardening — can be parallelized.
14. **§14** Test infrastructure — build alongside each feature; full harness last.
