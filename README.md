# GoMultiDB

A pragmatic distributed database prototype in Go.

This repository focuses on **incremental, test-backed system construction** across:
- server lifecycle and RPC foundations,
- storage/consensus/query scaffolding,
- CDC + xCluster replication workflows,
- control-plane operations,
- deployment, integration, stress, and CI-style verification.

---

## Current Status

The project has implemented and validated:

- Core server/runtime + RPC plumbing (`cmd/master`, `cmd/tserver`, `internal/server`, `internal/rpc`)
- Replication stack scaffolding:
  - CDC store/service/checkpoint flows
  - xCluster apply loop with retry/idempotency
  - control-plane stream/job registry + scheduler
- File-backed persistence for critical metadata surfaces
- Observability scaffolding (metrics/health/admin handlers)
- Deployment and testing toolchain:
  - Docker Compose cluster
  - real-infra integration harness
  - stress harness and thresholded reports
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
- `internal/query/{ysql,ycql,pggate}`
- `internal/replication/{cdc,xcluster,controlplane,observability}`


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

```powershell
./scripts/stress-run.ps1 -Profile quick
./scripts/stress-report.ps1
```

### 5) End-to-end CI-style local run

```powershell
./scripts/ci-run.ps1 -WithStress -StressProfile quick
```

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

Workspace gopls settings include these tags in `.vscode/settings.json`.

---

## Repository Structure (high-level)

- `cmd/` — binaries (`master`, `tserver`)
- `internal/` — core implementation
- `tests/integration/infra/` — compose-backed integration harness/tests
- `tests/stress/` — stress scenarios and reporting artifacts
- `scripts/` — operational and verification scripts


---

## Non-Goals (Current Iteration)

- Production SLO certification
- Full distributed network-fault simulation framework
- Complete WAL-derived CDC deep implementation parity
- Full split/remap orchestration and bootstrap automation depth

These are tracked as next-step roadmap items in task planning docs.

---

## Contributing Workflow

Recommended local sequence before proposing changes:

```powershell
./scripts/preflight.ps1
./scripts/test-integration-infra.ps1
./scripts/stress-run.ps1 -Profile quick
```

Keep changes incremental, deterministic, and test-backed.
