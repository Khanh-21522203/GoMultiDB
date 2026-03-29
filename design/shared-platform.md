# Shared Platform

### Purpose
Defines shared contracts (errors, IDs, envelopes/versioning, idempotency store) and platform-level node guards for filesystem layout and memory limit enforcement.

### Scope
**In scope:**
- Shared types in `internal/common/errors`, `ids`, `types`, `versioning`, `idempotency`.
- Platform substrate in `internal/platform/fsmanager.go` and `memtracker.go`.

**Out of scope:**
- Domain-specific business logic that consumes these contracts.

### Primary User Flow
1. Callers create typed request/transaction IDs and envelopes.
2. Components produce/normalize `DBError` values with retryability metadata.
3. Idempotent operations mark request fingerprints with TTL and validate replays.
4. Node startup initializes canonical FS directories and validates writable dirs.
5. Runtime subsystems consume memory through tracker and reject over-limit allocations.

### System Flow
1. `types.NewRequestEnvelope` sets `ContractVer` from `versioning.CurrentContractVersion`.
2. RPC/server components call `versioning.ValidateContractVersion` for strict contract checks.
3. `dberrors.NormalizeError` ensures non-DB errors map to `ErrInternalUnmapped`.
4. `idempotency.MemoryStore` stores `(scope, requestID) -> fingerprint + expiry` and enforces mismatch conflict.
5. `FSManager.Init` builds fixed subdirectory tree and optionally writes/verifies `node-instance.json` per data dir.
6. `MemTracker.Consume` checks root projected usage against hard limit and returns `ErrOOMKill` when exceeded.

### Data Model
- Error model:
  - `DBError {Code, Message, Retryable, Cause}`.
- Identity model:
  - `TxnID [16]byte`, `RequestID string`, typed node/table/tablet IDs.
- Request metadata:
  - `RequestEnvelope {RequestID, SourceNode, SentAt, ContractVer}`.
- Idempotency entry:
  - `memoryEntry {fingerprint, expiresAt}` keyed by `scope:requestID`.
- Platform models:
  - `NodeInstance {NodeID, Generation, CreatedAt}` persisted as `<dataDir>/node-instance.json`.
  - `MemTracker` tree with `hardLimitBytes` and propagated `currentUsage`.

### Interfaces and Contracts
- Error contracts:
  - `IsRetryable(err)` and `NormalizeError(err)`.
- ID contracts:
  - random generation via `NewTxnID/NewRequestID`; `Must*` variants panic on RNG failure.
- Idempotency contracts:
  - `Seen(scope,id)` and `Mark(scope,id,fingerprint,ttl)` with TTL > 0 requirement.
- FS platform contracts:
  - standard subdirs: `data`, `wals`, `tablet-meta`, `consensus-meta`, `snapshots`, `bootstrap_tmp`.

### Dependencies
**Internal modules:**
- Consumed by nearly all runtime/query/replication/master packages.

**External services/libraries:**
- Filesystem and JSON libs for node-instance persistence.
- `crypto/rand` for ID generation.

### Failure Modes and Edge Cases
- Invalid config/version checks typically surface as `ErrInvalidArgument`/`ErrInvalidConfig` in consumers.
- Idempotency mark with non-positive TTL returns `ErrInvalidArgument`.
- Fingerprint mismatch on active idempotency key returns `ErrIdempotencyConflict`.
- MemTracker over-limit consume returns `ErrOOMKill`.
- Node-instance mismatch across data directories returns error during startup validation.

### Observability and Debugging
- Debug points:
  - `errors/errors.go:NormalizeError`
  - `idempotency/store.go:Mark`
  - `platform/fsmanager.go:WriteNodeInstance`
  - `platform/memtracker.go:Consume`
- Tests:
  - `internal/platform/fsmanager_test.go`
  - `internal/platform/memtracker_test.go`

### Risks and Notes
- `MemTracker` tracks logical byte accounting by callers; under-reporting or missing release calls can skew enforcement.
- `FSManager` assumes first data/WAL directory for canonical tablet path helpers.

Changes:

