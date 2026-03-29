# Automation Tooling

### Purpose
Provides script-driven operational workflows for compose lifecycle, test tiers, integration runs, stress runs/reporting, and CI-stage orchestration.

### Scope
**In scope:**
- PowerShell scripts under `scripts/*.ps1`.
- Compose and container build configuration (`docker-compose.yml`, `docker-compose.override.yml`, `Dockerfile.master`, `Dockerfile.tserver`).
- Environment template `.env.example` used by compose defaults.

**Out of scope:**
- Internal package-level business logic executed by tests.

### Primary User Flow
1. Developer runs compose lifecycle scripts (`compose-up`, `compose-smoke`, `compose-status`, `compose-down`).
2. Developer runs quick/standard test scripts and optional integration/stress wrappers.
3. CI wrapper (`ci-run.ps1`) executes selected stages and optional stress profile.
4. Stress/report scripts export and summarize artifacts.

### System Flow
1. Compose scripts wrap `docker compose` commands and health checks for containers:
   - `multidb-master`, `multidb-tserver-1/2/3`.
2. Test scripts call:
   - `go test ./internal/master/balancer ./internal/testing/invariant ./internal/replication/cdc ./internal/replication/xcluster ./internal/server` (quick ownership/routing/regression surface)
   - `go test ./...` (standard)
   - tagged integration/stress suites.
3. Stress compose mode optionally brings cluster up with profile `stress`, runs tests, captures `compose-logs.txt`, then tears down unless `-KeepUp`.
4. CI script chains lint placeholder -> quick -> standard -> integration -> optional stress/report.

### Data Model
- Script parameter contracts:
  - `compose-up.ps1`: `-Profile`, `-Build`.
  - `compose-down.ps1`: `-RemoveVolumes`.
  - `stress-run.ps1`: `-Profile {quick|standard}`, `-Mode {local|compose}`, `-KeepUp`.
  - `ci-run.ps1`: `-WithStress`, `-StressProfile`.
- Compose service model:
  - services: `master`, `tserver-1`, `tserver-2`, `tserver-3`, optional `loadgen` (`stress` profile), optional `toxiproxy` (`fault` profile).
- Persistence:
  - script-generated outputs are test artifact files under `tests/**/artifacts`.
- Quick-check intent:
  - `scripts/test-quick.ps1` is topology-aware and validates post-Raft primary ownership balancing/invariant surfaces in addition to replication fast paths.

### Interfaces and Contracts
- Scripts assume PowerShell host and available Docker/Go toolchains.
- Integration and stress wrappers rely on build tags in Go tests.
- `controlplane-status.ps1` is informational (status guidance output), not an active API query.
- `controlplane-status.ps1` references ownership/failover diagnostics (`ownership_convergence`, `routing_consistency`) instead of Raft leader-specific checks.

### Dependencies
**Internal modules:**
- Scripts invoke package tests and harnesses documented in testing feature.

**External services/libraries:**
- Docker daemon + `docker compose`.
- Go toolchain in shell path.

### Failure Modes and Edge Cases
- Compose scripts fail when Docker daemon is unavailable.
- Stress compose mode explicitly checks Docker readiness and throws early if unavailable.
- Integration/stress wrappers can leave cluster running only when explicit keep-up flags are set.

### Observability and Debugging
- Script logs are prefixed (`[compose-*]`, `[quick]`, `[standard]`, `[stress]`, `[ci]`) for stage visibility.
- Debug starting points:
  - `scripts/ci-run.ps1`
  - `scripts/stress-run.ps1`
  - `scripts/test-integration-infra.ps1`

### Risks and Notes
- Scripts are PowerShell-first; non-PowerShell environments need manual equivalent commands.
- `ci-run.ps1` has a lint placeholder stage and does not enforce static analysis beyond tests.

Changes:
