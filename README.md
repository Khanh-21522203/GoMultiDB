# GoMultiDB

A pragmatic distributed database prototype in Go.

This repository focuses on **incremental, test-backed system construction** across:
- server lifecycle and RPC foundations,
- storage/consensus/query scaffolding,
- master catalog, heartbeat, and snapshotting,
- CDC + xCluster replication workflows,
- control-plane operations,
- deployment, integration, stress, and CI-style verification.

---

## Current Status

The project has implemented and validated:

**Master Control Plane:**
- Catalog manager with sys.catalog backing (`internal/master/catalog`, `internal/master/syscatalog`)
- Heartbeat service with tablet tracking and leader election (`internal/master/heartbeat`)
- Distributed snapshot coordinator (`internal/master/snapshot`)
- Load balancer with placement planning (`internal/master/balancer`)
- Snapshotting and backup orchestration (create/delete/restore snapshots)

**Tablet Lifecycle:**
- Tablet manager with durable state markers (`internal/tablet/manager`, `internal/tablet/meta_store`)
- Remote bootstrap client and session (`internal/tablet/remotebootstrap`)
- Tablet snapshot handlers for checkpoint/restore (`internal/tablet/snapshot`)

**Replication Stack:**
- CDC store/service/checkpoint flows
- xCluster apply loop with retry/idempotency
- Control-plane stream/job registry + scheduler
- File-backed persistence for critical metadata surfaces

**Query Layer Scaffolding:**
- SQL/CQL coordinators and dispatch foundations
- PgGate bridge with executor framework
- YSQL/YCQL server stubs with health status

**Observability:**
- Metrics/health/admin handlers
- Stress harness and thresholded reports

**Deployment & Testing:**
- Docker Compose cluster
- Real-infra integration harness
- In-process test cluster (`internal/testing/cluster`)
- CI-style test orchestration scripts

> Note: this is a scaffold/hardening-oriented system build, not a production-certified database.

---

## Architecture at a Glance

Primary runtime roles:
- `master`: metadata/coordination-oriented node process
- `tserver`: data/query/replication-oriented node process

Primary internal domains:
- `internal/rpc` — transport/contracts
- `internal/server` — runtime/lifecycle/config
- `internal/raft`, `internal/wal`, `internal/docdb`, `internal/storage/rocks`
- `internal/query/{sql,cql,pggate}` — query layer foundations
- `internal/replication/{cdc,xcluster,controlplane,observability}` — replication stack
- `internal/master/{catalog,heartbeat,snapshot,balancer,syscatalog,registry}` — master control plane
- `internal/tablet/{manager,remotebootstrap,snapshot}` — tablet lifecycle and operations

---

## Architecture Diagram

```text
                           +--------------------+
                           |      Clients       |
                           | (SQL / CQL APIs)   |
                           +---------+----------+
                                     |
                                     v
+---------------------+     +--------+--------+     +----------------------+
|     master node     |<--->|   RPC / Server  |<--->|    tserver nodes     |
| cmd/master          |     |  infrastructure  |     | cmd/tserver (N)      |
| metadata/control    |     | internal/rpc     |     | query + replication  |
+----------+----------+     +--------+--------+     +----------+-----------+
           |                          |                         |
           |                          |                         |
           v                          v                         v
+----------+----------+    +----------+-----------+   +--------+----------------+
| control-plane       |    | observability        |   | storage/consensus layer |
| stream/job registry |    | metrics/health/admin |   | raft + wal + docdb      |
| scheduler           |    | handlers             |   | rocks                   |
+----------+----------+    +----------------------+   +--------+----------------+
           |
           v
+----------+---------------------------------------------------------------+
|                   replication subsystem                                  |
|   CDC service/store/checkpoints  --->  xCluster apply loop              |
|   retention/split/bootstrap APIs --->  retry/idempotency/lag snapshots  |
+--------------------------------------------------------------------------+
```

## Core Flows

### 1) Write/Apply Replication Flow

```text
Event Append -> CDC Store -> GetChanges (Producer path) -> xCluster ApplyLoop
           -> checkpoint advance -> lag snapshot/status update
```

### 2) Control-Plane Scheduling Flow

```text
Scheduler Tick
  -> list jobs/streams
  -> filter RUNNING jobs on RUNNING streams
  -> poll CDC after checkpoint
  -> apply batch with in-flight cap
  -> checkpoint progression
  -> deterministic fault injection path (optional)
```

### 3) Retention / Recovery Flow

```text
EvaluateRetention
  -> OK                    => continue replication
  -> BOOTSTRAP_REQUIRED    => mark bootstrap state REQUIRED
                            -> Start/Complete/Fail bootstrap lifecycle
```

### 4) Distributed Snapshotting Flow

```text
SnapshotCoordinator.CreateSnapshot(tabletIDs)
  -> TabletRPCRegistry.GetEndpoint(tabletID)
      -> ReconcileSink.GetTablet() + TSManager.Get(tsUUID)
      -> returns http://<ts-address>:<rpc-port>
  -> HTTP RPC to tablet servers (parallel fan-out)
      -> TabletSnapshotService.CreateTabletSnapshot()
      -> TabletSnapshotStore.ScanDB() + persistSnapshotData()
      -> checkpoint written to snapshot/<snapshotID>/<tabletID>/
  -> Snapshot metadata persisted to master store
      -> key: snapshot/<snapshotID>, value: SnapshotRecord
  -> On completion: state transition CREATING -> COMPLETE
```

Delete flow:
```text
SnapshotCoordinator.DeleteSnapshot(snapshotID)
  -> state transition CREATING/COMPLETE -> DELETING
  -> Fan-out DeleteTabletSnapshot to all tablet replicas
  -> TabletSnapshotStore.deleteSnapshotData()
  -> Remove metadata from master store
```

Restore flow:
```text
SnapshotCoordinator.RestoreSnapshot(snapshotID)
  -> state transition COMPLETE -> RESTORING
  -> Fan-out RestoreTabletSnapshot to all tablet replicas
  -> TabletSnapshotStore loads checkpoint data back to RegularDB
  -> On completion: state transition RESTORING -> COMPLETE
```

### 5) Deployment & Verification Flow

```text
Compose Up -> Health Wait -> Integration Tests -> Stress Tests -> CI Orchestration
```

---

## Quick Start

### 1) Unit test tiers

```powershell
./scripts/test-quick.ps1
./scripts/test-standard.ps1
```

### 2) Compose deployment (local cluster)

```powershell
./scripts/compose-up.ps1 -Build
./scripts/compose-smoke.ps1
./scripts/compose-status.ps1
```

Teardown:

```powershell
./scripts/compose-down.ps1
```

### 3) Real-infra integration suite

```powershell
./scripts/test-integration-infra.ps1
```

### 4) Stress suite

Local deterministic mode:

```powershell
./scripts/stress-run.ps1 -Profile quick -Mode local
./scripts/stress-report.ps1
```

Container-backed mode (recommended for realistic system behavior):

```powershell
./scripts/stress-run.ps1 -Profile quick -Mode compose
./scripts/stress-report.ps1
```

### 5) End-to-end CI-style local run

```powershell
./scripts/ci-run.ps1 -WithStress -StressProfile quick
```

### 6) Stress scenarios (simple explanation)

Use this section as a quick mental model:

- `S1_steady_state` = normal traffic baseline
- `S2_bursty_with_faults` = traffic spikes + injected faults
- `S3_scale_out` = many streams/jobs at the same time
- `S4_soak` = long-running stability check

What each metric means (and how it maps to code):
- `throughput` (events/sec)
  - Computed in `tests/stress/scenarios_test.go` as `processed_events / duration_seconds`.
  - Reflects effective flow through `internal/replication/cdc` -> `internal/replication/xcluster`.

- `retry_ratio` (0.00 to 1.00)
  - Computed in burst/fault scenarios as `fault_ticks / total_ticks`.
  - Fault ticks come from deterministic scheduler fault injection in `internal/replication/controlplane/scheduler.go` (`FailureEveryTicks`).

- `lag_upper` (events)
  - Represents max observed backlog (produced sequence minus applied/checkpointed progress).
  - Tied to checkpoint progression in `internal/replication/cdc` and scheduler/apply advancement.

- `checkpoint_staleness` (events)
  - Represents how far checkpoint is behind latest produced sequence at scenario end.
  - Derived from checkpoint reads in stress scenarios + CDC checkpoint state.

Pass rule (simplified):
- A scenario passes when all its metrics stay within configured thresholds.
- The full stress run passes when all 4 scenarios pass.

Quick profile workload sizes:
- `S1`: streams=4, events_per_stream=200, iterations=100
- `S2`: streams=2, events_per_stream=200, iterations=100, fault_every_n=25
- `S3`: streams=8, events_per_stream=100, iterations=100
- `S4`: soak_duration_seconds=10

Standard profile workload sizes:
- `S1`: streams=12, events_per_stream=1000, iterations=400
- `S2`: streams=6, events_per_stream=1000, iterations=400, fault_every_n=25
- `S3`: streams=24, events_per_stream=400, iterations=400
- `S4`: soak_duration_seconds=30

### 7) Latest verified stress sample (compose mode)

From the latest successful run:

- Runtime: `TestStressScenarios` completed in `94.47s`
- Seed: `20260307`
- Mode: `compose`
- Overall result: `PASS`

Per-scenario reading guide:
- `S1_steady_state`: `throughput=15549.05`, `retry_ratio=0`, `lag_upper=0`
  - Meaning: baseline path is healthy and has no backlog.
- `S2_bursty_with_faults`: `throughput=18596.62`, `retry_ratio=0.04`, `lag_upper=0`
  - Meaning: some retries happened (expected under faults), but replication kept up.
- `S3_scale_out`: `throughput=15804.15`, `retry_ratio=0`, `lag_upper=10`
  - Meaning: mild backlog under concurrency growth, still within threshold.
- `S4_soak`: `throughput=1745.87`, `retry_ratio=0`, `lag_upper=0`
  - Meaning: long-run stability is good; no lag accumulation.

Practical takeaway:
- `local` mode = faster developer feedback.
- `compose` mode = more realistic multi-service behavior and better for benchmarking.

---

## Build Tags (Go + Editor)

Some test suites are build-tagged:
- `integration` for real-infra tests
- `stress` for stress suites

Examples:

```powershell
go test ./tests/integration/infra -tags integration -v
go test ./tests/stress -tags stress -v
```

---

## Repository Structure (high-level)

- `cmd/` — binaries (`master`, `tserver`)
- `internal/` — core implementation
- `tests/integration/infra/` — compose-backed integration harness/tests
- `tests/stress/` — stress scenarios and reporting artifacts
- `scripts/` — operational and verification scripts

---

## Contributing Workflow

Recommended local sequence before proposing changes:

```powershell
./scripts/preflight.ps1
./scripts/test-integration-infra.ps1
./scripts/stress-run.ps1 -Profile quick
```

Keep changes incremental, deterministic, and test-backed.
