# Testing Infrastructure

### Purpose
Provides reusable in-process testing harnesses plus integration/stress suites that exercise runtime, replication, and query paths with artifact capture.

### Scope
**In scope:**
- In-process harness packages under `internal/testing/*`.
- Docker-compose integration harness under `tests/integration/infra`.
- Stress harness and scenarios under `tests/stress`.
- Artifact files committed under `tests/*/artifacts`.

**Out of scope:**
- Production runtime logic itself.

### Primary User Flow
1. Developer runs quick/standard/integration/stress scripts.
2. In-process tests use cluster/fault/invariant/workload helpers.
3. Integration tests spin real compose cluster and execute critical-path scenarios.
4. Stress tests run scenario profiles and write summary/config artifacts for regression checks.

### System Flow
1. In-process cluster path (`internal/testing/cluster`):
   - creates temporary node dirs
   - boots `server.Runtime` + in-memory stores
   - tears down all nodes and temp data.
2. Fault engine path (`internal/testing/fault`) injects partition/kill/delay/diskfull/clockskew state queried by test interceptors.
3. Invariant checker (`internal/testing/invariant`) pulls read-only inspector state and asserts no-lost-write/convergence/no-double-apply invariants.
4. Workload runner (`internal/testing/workload`) executes throttled concurrent read/write ops and computes percentile/throughput stats.
5. Integration harness path (`tests/integration/infra/harness.go`) manages compose lifecycle, health waiting, retries, and summary/log artifact writes.
6. Stress harness path (`tests/stress/harness.go` + `scenarios_test.go`) evaluates thresholded scenarios and writes `summary.json` + `config.json`.

### Data Model
- In-process cluster models:
  - `TestClusterSpec`, `NodeHandle`, `ClusterHandle`.
- Fault model:
  - `FaultAction {FaultID, Type, FromNode, ToNode, Value}`.
- Invariant model:
  - `WriteRecord`, `TabletReplicaState`, `ClusterInspector` interface.
- Workload model:
  - `WorkloadSpec`, `WorkloadResult`.
- Integration/stress result models:
  - `infra.Summary`, `stress.Summary`, `stress.ScenarioResult`.
- Artifact persistence:
  - Integration: `tests/integration/infra/artifacts/{summary.json,status-snapshot.json,compose-logs.txt}`.
  - Stress: `tests/stress/artifacts/{summary.json,config.json,compose-logs.txt}`.

### Interfaces and Contracts
- Invariant dispatch API: `AssertInvariant(name, inspector, opts...)` supports `no_lost_writes`, `replica_convergence`, `no_uncommitted_intents`, `no_double_apply`.
- Invariant dispatch API: `AssertInvariant(name, inspector, opts...)` supports `no_lost_writes`, `ownership_convergence`, `routing_consistency`, `durability_no_pending_intents`, `durability_no_double_apply` (legacy names remain aliases for compatibility).
- Workload contract: target must implement `Read(ctx,tableID)` and `Write(ctx,tableID)`.
- Integration/stress suites are build-tag gated:
  - `//go:build integration` for infra suite.
  - `//go:build stress` for stress suite.

### Dependencies
**Internal modules:**
- Runtime, replication, and query packages are exercised by integration/stress scenarios.

**External services/libraries:**
- Docker/Compose CLI for infra and compose-mode stress runs.

### Failure Modes and Edge Cases
- Integration harness health wait timeout returns error after configured retries/window.
- Stress threshold evaluation marks scenarios failed when throughput/retry/lag/staleness bounds are violated.
- Workload runner respects context timeout and drains workers before returning final stats.

### Observability and Debugging
- Artifacts are primary debugging output (logs + JSON summaries) in both infra and stress suites.
- Debug points:
  - `tests/integration/infra/infra_integration_test.go`
  - `tests/stress/scenarios_test.go`
  - `internal/testing/*` helper package entry points.
- Primary ownership transfer/restart regression:
  - `internal/server/phase5_failover_smoke_test.go:TestPhase5PrimaryOwnershipTransferRecovery`.

### Risks and Notes
- Compose-backed suites depend on local Docker availability and can fail for environment reasons unrelated to code logic.
- Artifact files in repo are snapshots from prior runs and may not reflect current HEAD behavior.

Changes:
