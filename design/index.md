# GoMultiDB — Design Index

GoMultiDB is a Go-based distributed database prototype with two process roles (`master` and `tserver`) that run RPC endpoints plus query front-ends (CQL and SQL scaffolding), and compose in-memory storage, replication, and control-plane modules. Runtime entrypoints are Go CLIs under `cmd/`, with Docker Compose and PowerShell scripts for local cluster orchestration, integration tests, and stress runs.

## Mental Map
```
┌─ Runtime And Transport ────────────────────────────────────────────────┐
│ Owns: process bootstrap, lifecycle, RPC server/client, ping service    │
│ Entry: cmd/master/main.go, cmd/tserver/main.go                         │
│ Key:   internal/server, internal/rpc, internal/services/ping           │
│ Uses:  Query Layer, Master Control Plane, Tablet Services              │
└────────────────────────────────────────────────────────────────────────┘
┌─ Query Layer ───────────────────────────────────────────────────────────┐
│ Owns: CQL protocol listener/execution, SQL managed coordinator, PgGate   │
│ Entry: internal/query/cql/listener.go, internal/query/sql/managed_coordinator.go│
│ Key:   internal/query/cql, internal/query/sql, internal/query/pggate    │
│ Uses:  Shared And Platform, Transactions, Tablet Services               │
└─────────────────────────────────────────────────────────────────────────┘
┌─ Master Control Plane ──────────────────────────────────────────────────┐
│ Owns: catalog state, heartbeats, snapshot coordination, balancing       │
│ Entry: internal/master/heartbeat/service.go                             │
│ Key:   internal/master/catalog, internal/master/heartbeat, internal/master/snapshot │
│ Uses:  Tablet Services, Storage Foundation                              │
└─────────────────────────────────────────────────────────────────────────┘
┌─ Tablet Services ───────────────────────────────────────────────────────┐
│ Owns: tablet lifecycle transitions, partition splits, remote bootstrap  │
│ Entry: internal/tablet/manager.go                                       │
│ Key:   internal/tablet/*, internal/partition/map.go                     │
│ Uses:  Storage Foundation, Master Control Plane                         │
└─────────────────────────────────────────────────────────────────────────┘
┌─ Replication ───────────────────────────────────────────────────────────┐
│ Owns: CDC stream store/service, xCluster apply, control-plane scheduler │
│ Entry: internal/replication/cdc/service.go                              │
│ Key:   internal/replication/cdc, xcluster, controlplane, observability  │
│ Uses:  Storage Foundation, Shared And Platform                          │
└─────────────────────────────────────────────────────────────────────────┘
┌─ Storage Foundation ──────────────────────────────────────────────────────┐
│ Owns: WAL, in-memory Rocks KV facade, DocDB MVCC/intents                 │
│ Entry: internal/wal/log.go, internal/docdb/engine.go                     │
│ Key:   internal/wal, internal/storage/rocks, internal/docdb              │
│ Uses:  Transactions, Shared And Platform                                  │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Transactions ──────────────────────────────────────────────────────────┐
│ Owns: txn state machine, conflict resolution, wait queues, deadlocks    │
│ Entry: internal/txn/manager.go                                          │
│ Key:   internal/txn/manager.go, conflict, waitq                         │
│ Uses:  Storage Foundation                                               │
└─────────────────────────────────────────────────────────────────────────┘
┌─ Shared And Platform ───────────────────────────────────────────────────┐
│ Owns: error/ID/version contracts, idempotency TTL store, FS/mem guards  │
│ Key:   internal/common/*, internal/platform/*                           │
│ Uses:  (shared by all domains)                                          │
└─────────────────────────────────────────────────────────────────────────┘
┌─ Testing And Ops ────────────────────────────────────────────────────────┐
│ Owns: in-process cluster/fault/invariant/workload harnesses, infra tests │
│ Key:   internal/testing/*, tests/integration/infra, tests/stress, scripts│
│ Uses:  Runtime And Transport, Replication, Query Layer                   │
└──────────────────────────────────────────────────────────────────────────┘
```
## Feature Matrix

| Feature | Description | File | Status |
|---------|-------------|------|--------|
| Runtime Bootstrap | Bootstraps master/tserver runtimes, primary-owner wiring, and node config defaults | [runtime-bootstrap.md](runtime-bootstrap.md) | Stable |
| RPC Services | HTTP JSON-RPC transport plus ping RPC contract | [rpc-services.md](rpc-services.md) | Stable |
| CQL Gateway | CQL protocol framing, listener, prepared cache, and core DML execution engine | [query-cql-gateway.md](query-cql-gateway.md) | In Progress |
| SQL Control | Managed SQL coordinator with optional postgres subprocess mode and fallback | [query-sql-control.md](query-sql-control.md) | In Progress |
| PgGate | PgGate session/transaction bridge with default in-process resolver/dispatcher/coordinator | [query-pggate.md](query-pggate.md) | In Progress |
| Master Catalog Heartbeat | Table/tablet catalog mutations, heartbeat reconciliation, and syscatalog persistence | [master-catalog-heartbeat.md](master-catalog-heartbeat.md) | Stable |
| Master Snapshot Coordinator | Distributed snapshot coordinator and master snapshot RPC/service/store paths | [master-snapshot-coordinator.md](master-snapshot-coordinator.md) | Stable |
| Master Balancer | Replica/primary ownership planner and cooldown logic | [master-balancer.md](master-balancer.md) | Stable |
| Tablet Lifecycle | Tablet state machine with ownership epoch transfer semantics and split orchestration | [tablet-lifecycle.md](tablet-lifecycle.md) | Stable |
| Tablet Snapshot Bootstrap | Tablet-level snapshot copy/restore and remote bootstrap transfer sessions | [tablet-snapshot-bootstrap.md](tablet-snapshot-bootstrap.md) | Stable |
| CDC Replication | CDC event store, stream API, checkpoint persistence, split remapping | [replication-cdc.md](replication-cdc.md) | Stable |
| xCluster Apply | Event apply loop with retries, dedupe, and checkpoint advancement | [replication-xcluster.md](replication-xcluster.md) | Stable |
| Replication Control Plane | Stream/job registry snapshots, ownership-epoch signals, and scheduler ticks/in-flight caps | [replication-controlplane.md](replication-controlplane.md) | Stable |
| Replication Observability | Metrics registry, ownership-transition metrics, health endpoints, and admin handlers | [replication-observability.md](replication-observability.md) | Stable |
| Transaction Coordinator | Transaction record lifecycle, conflict policy, wait queue deadlock abort | [txn-coordinator.md](txn-coordinator.md) | Stable |
| Storage Foundation | DocDB MVCC/intents on top of Rocks memory KV and WAL segments | [storage-foundation.md](storage-foundation.md) | Stable |
| Shared Platform | Shared error/ID/type/version contracts and platform FS/memory guards | [shared-platform.md](shared-platform.md) | Stable |
| Testing Infrastructure | In-process harnesses with ownership/routing invariants, infra integration suite, stress harness | [testing-infrastructure.md](testing-infrastructure.md) | Stable |
| Automation Tooling | Compose/test/stress PowerShell scripts and deployment scaffolding | [automation-tooling.md](automation-tooling.md) | Stable |

## Cross-Cutting Concerns

- Error handling uses `internal/common/errors.DBError` codes with explicit retryability; transport and subsystems normalize unknown errors via `NormalizeError`.
- Request-level contract compatibility is enforced by `types.RequestEnvelope.ContractVer` and `versioning.ValidateContractVersion` in `internal/rpc/server.go`.
- Idempotency patterns appear in master catalog request dedupe and PgGate request fingerprinting (`internal/common/idempotency/store.go`, `internal/master/catalog/manager.go`, `internal/query/pggate/executor.go`).
- Most services accept `context.Context` and short-circuit on cancellation; scheduler, polling loops, and subprocess wrappers rely on context/timeouts.
- Persistence is file/KV-key based rather than SQL schema based: examples include `reqlog/*`, `entity/*`, `snapshot/*`, checkpoint JSON files, tablet meta files, and WAL segments.
- Tests rely on both in-process harnesses (`internal/testing/*`) and Docker Compose-backed integration (`tests/integration/infra`) plus stress runs (`tests/stress`).

## Notes

- Removed feature doc `raft-consensus.md` during re-create sync because `internal/raft` no longer exists in current code.
