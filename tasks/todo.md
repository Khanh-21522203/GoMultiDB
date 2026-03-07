# Deployment + Real-Infra Integration & Stress Testing Plan

## Context

- Core replication/query/control-plane scaffolding is implemented and test-backed.
- Next objective is production-like deployment + real infrastructure integration coverage.
- This plan focuses on executable artifacts (compose stack, test harness, stress suite, CI hooks).

---

## Track A — Docker Compose Deployment (Detailed)

### A1. Containerization Foundations
- [x] Add `Dockerfile.master` for `cmd/master` image build.
- [x] Add `Dockerfile.tserver` for `cmd/tserver` image build.
- [x] Add common base image strategy (multi-stage build, static/minimal runtime image).
- [x] Add runtime env/config contract for both roles (ports, bind addresses, node ids, data dirs).
- [x] Add healthcheck command strategy per service (HTTP or lightweight probe).

### A2. Compose Topology Definition
- [x] Create `docker-compose.yml` for baseline cluster topology:
  - [x] 1 master
  - [x] 3 tservers
  - [x] shared network
  - [x] persistent volumes per node
- [x] Expose required ports for RPC/admin/diagnostic endpoints.
- [x] Add deterministic startup ordering (`depends_on` + healthcheck readiness).
- [x] Add restart policy and resource limits (CPU/memory) defaults.

### A3. Config Profiles & Overrides
- [x] Add `docker-compose.override.yml` for local dev overrides.
- [x] Add optional profile for stress mode (higher replicas/resources).
- [x] Add optional profile for fault mode (intentional delay/latency injection placeholders).
- [x] Add environment file template (`.env.example`) with all compose vars documented.

### A4. Operational Tooling for Compose
- [x] Add scripts:
  - [x] `scripts/compose-up.ps1`
  - [x] `scripts/compose-down.ps1`
  - [x] `scripts/compose-reset.ps1` (safe volume cleanup)
  - [x] `scripts/compose-logs.ps1` (filtered tail by service)
- [x] Add `scripts/compose-status.ps1` to summarize service health.
- [x] Add smoke checks script to validate cluster readiness after startup.

### A5. Compose Docs & Runbook
- [x] Add `tasks/deployment-compose-runbook.md`:
  - [x] startup flow
  - [x] expected healthy states
  - [x] troubleshooting guide (ports, stale volumes, crash loops)
  - [x] teardown/reset guidance
- [x] Add command snippets for Windows PowerShell usage.

---

## Track B — Real-Infra Integration Tests (Critical Paths)

### B1. Integration Harness Architecture
- [x] Add dedicated integration test package/folder (`tests/integration/infra`).
- [x] Define harness lifecycle helpers:
  - [x] start compose stack
  - [x] wait for health readiness
  - [x] seed baseline data
  - [x] clean shutdown + artifact capture
- [x] Add deterministic timeout/retry wrappers for infra tests.

### B2. Critical Path Coverage Matrix
- [x] Define and implement integration tests for:
  - [x] server lifecycle: start/stop/health transitions
  - [x] CDC stream create/getChanges/setCheckpoint/delete lifecycle
  - [x] xCluster apply loop end-to-end with checkpoint advance
  - [x] control-plane stream/job pause/resume/stop behavior against running infra
  - [x] query-layer smoke through running services (PgGate/YCQL minimal paths)
  - [x] restart recovery using persisted checkpoint/control-plane metadata
- [x] Add assertions for canonical error behavior on bad requests.

### B3. Failure/Recovery Integration Paths
- [x] Add tests for:
  - [x] transient service restart during active apply
  - [x] delayed/unavailable node then recovery (deterministic restart/rejoin path)
  - [x] checkpoint persistence continuity after container restart
  - [x] scheduler fault injection with real infra orchestration context
- [x] Ensure each failure test has deterministic pass/fail condition.

### B4. Integration Test Artifacts
- [x] Capture logs/artifacts on failures:
  - [x] compose service logs
  - [x] selected status snapshots (control-plane/observability)
  - [x] test-run summary report
- [x] Add machine-readable summary output (JSON/TXT) for CI parsing.

---

## Track C — Stress & Load Testing

### C1. Stress Harness Setup
- [x] Add stress test package (`tests/stress`).
- [x] Add configurable load generator for:
  - [x] CDC event append/poll pressure
  - [x] xCluster apply throughput pressure
  - [x] control-plane job/stream scaling pressure
- [x] Add deterministic seed + scenario config file.

### C2. Stress Scenarios (Critical)
- [x] Scenario S1: steady-state high-throughput CDC/apply.
- [x] Scenario S2: bursty load with intermittent injected faults.
- [x] Scenario S3: scale-out streams/jobs (cardinality growth).
- [x] Scenario S4: long-running soak (memory/leak/stability check window).

### C3. Stress Metrics & Acceptance Thresholds
- [x] Define thresholds for:
  - [x] sustained throughput
  - [x] retry/failure ratios
  - [x] lag upper bounds
  - [x] checkpoint staleness
- [x] Add threshold assertions in stress result parser.
- [x] Add pass/fail summary output with top offenders.

### C4. Stress Execution Scripts
- [x] Add scripts:
  - [x] `scripts/stress-run.ps1`
  - [x] `scripts/stress-report.ps1`
- [x] Add quick stress profile (developer machine friendly).
- [x] Add standard stress profile (CI/nightly friendly).

---

## Track D — CI/Automation Integration

### D1. Test Tiering
- [x] Keep existing quick/standard unit tiers.
- [x] Add infra-integration tier command entrypoint.
- [x] Add stress tier command entrypoint (gated/nightly).

### D2. Pipeline Contract (scaffold)
- [x] Define pipeline order:
  - [x] lint
  - [x] unit quick
  - [x] unit standard
  - [x] integration infra
  - [x] optional stress/nightly
- [x] Define artifact retention policy for failing integration/stress jobs.

### D3. Flakiness Controls
- [x] Add retry budget policy for infra bring-up failures.
- [x] Add flaky-test quarantine checklist/process.
- [x] Add deterministic random seed logging in all stress tests.

---

## Track E — Verification & Exit Gates

### E1. Mandatory Verification
- [x] `go test ./...` passes.
- [x] Compose stack boots and health checks pass locally.
- [x] Critical-path integration suite passes.
- [x] At least one stress scenario passes with thresholds.
- [x] Lints clean on touched files.

### E2. Documentation Completion
- [x] Deployment runbook finalized.
- [x] Integration matrix documented.
- [x] Stress thresholds and interpretation guidance documented.

### E3. Final Readiness Review
- [x] Capture known limitations/non-goals.
- [x] Record deferred items for next iteration.
- [x] Produce final deployment + testing readiness summary.
