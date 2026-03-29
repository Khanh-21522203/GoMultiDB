# Raft Consensus

### Purpose
Implements raft-like leader election/lease mechanics, append/vote/pre-vote handling, dynamic peer membership changes, and metadata durability.

### Scope
**In scope:**
- Consensus state machine in `internal/raft/consensus.go`.
- Metadata persistence in `internal/raft/metadata_store.go`.
- Replication queue buffer in `internal/raft/replication_queue.go`.

**Out of scope:**
- Network transport for raft RPC (this package exposes handler methods only).

### Primary User Flow
1. Node creates consensus object with WAL + metadata store.
2. Election timer drives follower-to-candidate transitions.
3. Leader handles writes via `Replicate`; followers handle append/vote requests.
4. Operator/control loop can apply membership change operations.

### System Flow
1. `NewConsensus` loads metadata (`CurrentTerm`, `VotedFor`, peer list), initializes state and queue.
2. `Start` runs election timer loop; on timeout non-leader becomes candidate and increments term.
3. `Replicate` (leader-only) appends entries to WAL, enqueues for replication, updates committed/applied IDs, renews lease.
4. `HandleAppendEntries` and `HandleRequestVote` enforce term and up-to-date log rules and persist metadata when term/vote changes.
5. `ReadAtLease` allows leader-lease-based reads without quorum round-trip.
6. `ChangeConfig` applies one peer add/remove at a time, persisting new metadata before in-memory swap.

### Data Model
- `ReplicaState`:
  - `CurrentTerm`, `VotedFor`, `Role`, `LeaderID`, `CommittedOpID`, `LastAppliedOpID`, `LastReceivedOpID`, `LeaderLeaseUntil`, `ConfigVersion`.
- RPC models:
  - `VoteRequest/Response`, `AppendEntriesRequest/Response`, `PreVoteRequest/Response`.
- Metadata persistence model:
  - `Metadata {CurrentTerm, VotedFor, ConfigVersion, Peers []string}`.
- Persistence file:
  - `<metadata-dir>/consensus-meta.json` via atomic temp file replacement.

### Interfaces and Contracts
- Consensus APIs:
  - `Start`, `Stop`, `State`, `Replicate`, `HandleAppendEntries`, `HandleRequestVote`, `HandlePreVote`, `ReadAtLease`, `ChangeConfig`, `Peers`.
- Leader lease contract:
  - `Replicate` and `ReadAtLease` require valid leader role/lease.
- Membership change contract:
  - only leader can change config, self-ID changes rejected, one config change in-flight.

### Dependencies
**Internal modules:**
- `internal/wal` for durable ordered log entries.
- `internal/common/errors` and `internal/common/types` for errors/op IDs.

**External services/libraries:**
- Filesystem I/O for metadata file store.

### Failure Modes and Edge Cases
- Replicate from non-leader returns `ErrNotLeader`.
- Lease expiry returns `ErrTimeout` (replicate) or `ErrLeaseExpired` (read-at-lease).
- Term/vote metadata persistence failure returns wrapped internal error.
- Invalid config change operations return `ErrInvalidArgument` or `ErrConflict`.
- Follower behind bootstrap boundary sets `AppendEntriesResponse.NeedsBootstrap=true`.

### Observability and Debugging
- Debug points:
  - `consensus.go:Replicate`, `HandleAppendEntries`, `onElectionTimeout`, `ChangeConfig`.
  - `metadata_store.go:Load/Save`.
- Tests:
  - `consensus_test.go`
  - `raft_features_test.go`

### Risks and Notes
- Election RPC exchange/quorum mechanics are represented as handlers; integration with transport and peer messaging is outside this package.
- Replication queue is in-memory and not persisted.

Changes:

