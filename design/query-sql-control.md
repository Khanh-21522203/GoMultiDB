# SQL Control

### Purpose
Provides SQL process lifecycle abstractions: a managed coordinator that can run a node-local `PGProcess` subprocess and automatically fall back to local coordinator mode when process startup is unavailable.

### Scope
**In scope:**
- `LocalCoordinator` start/stop/health contract in `internal/query/sql/coordinator.go`.
- `ManagedCoordinator` process-preferred orchestration/fallback behavior in `managed_coordinator.go`.
- `PGProcess` init/start/stop/health and config file generation in `pgprocess.go`.
- Catalog version tracker used for cache invalidation in `pgprocess.go`.
- Runtime and CLI wiring for process mode flags in `internal/server/runtime.go` and `cmd/*`.

**Out of scope:**
- SQL query parsing/plan execution.
- Master catalog/table metadata semantics.

### Primary User Flow
1. Runtime starts SQL path with `ProcessConfig`.
2. `ManagedCoordinator` prefers `PGProcess` when `PreferProcess=true`.
3. If process mode cannot start and `AllowCoordinatorFallback=true`, coordinator transparently falls back to `LocalCoordinator`.
4. Health/status calls are delegated to the active mode (process or local coordinator).

### System Flow
1. Entry point from runtime: `internal/server/runtime.go:Start` calls `sqlCoord.Start(ctx, ProcessConfig{...})` from `Runtime.sqlProcessConfig()`.
2. `ManagedCoordinator.Start`:
   - validates SQL bind and connection settings.
   - if process mode is enabled, builds `PGProcessConfig` from `ProcessConfig` and tries to start process mode.
   - falls back to `LocalCoordinator` when process startup fails and fallback is allowed.
3. `PGProcess.Start` path:
   - `InitDB` (creates data dir if missing, runs `initdb`).
   - Writes `postgresql.conf` and `pg_hba.conf`.
   - Launches `postgres` process and polls TCP readiness.
4. `ManagedCoordinator.Stop` delegates to either process stop or local coordinator stop.

### Data Model
- `ProcessConfig`:
  - `Enabled`, `BindAddress`, `MaxConnections`, `HBAConfig`, `ExtraConf`.
  - `PreferProcess`, `ProcessDataDir`, `ProcessBinPath`, `ProcessInitDBPath`.
  - `ProcessStartTimeout`, `ProcessStopTimeout`, `AllowCoordinatorFallback`.
- `PGProcessConfig`:
  - `DataDir`, `BindAddress`, `Port`, `BinPath`, `InitDBPath`, `StartTimeout`, `StopTimeout`, `HBAConfigContent`, `ExtraConf`.
- `CatalogVersion` tracker:
  - `version (uint64)` with `Get/Increment/Set`.
- Persistence:
  - Process config files are written to `DataDir/postgresql.conf` and `DataDir/pg_hba.conf`.

### Interfaces and Contracts
- `Coordinator` interface: `Start`, `Stop`, `Health`.
- `LocalCoordinator.Start` idempotent only for same config; differing second start returns `ErrConflict`.
- `ManagedCoordinator.Start` is idempotent only for same config and either:
  - starts `PGProcess` (when enabled and available), or
  - starts local fallback coordinator.
- `Health` returns `ErrRetryableUnavailable` if not started.
- `PGProcess` contract:
  - Requires `DataDir`.
  - Defaults to `127.0.0.1:5433`, start timeout 30s, stop timeout 10s.
- Runtime config contract (`internal/server/config.go`):
  - `EnableSQLProcess`, `SQLDataDir`, `SQLProcessBinPath`, `SQLProcessInitDBPath`, `SQLAllowFallbackToCoordinator`.
- CLI flags (`cmd/master`, `cmd/tserver`):
  - `--enable-sql-process`, `--sql-process-required`, `--sql-data-dir`, `--sql-bin-path`, `--sql-initdb-path`.

### Dependencies
**Internal modules:**
- `internal/common/errors` for typed error codes.

**External services/libraries:**
- `os/exec` for running `initdb` and `postgres` binaries.
- TCP probe via `net.DialTimeout`.

### Failure Modes and Edge Cases
- Missing bind address when enabled -> `ErrInvalidArgument`.
- Start with different config while started -> `ErrConflict`.
- `initdb` or postgres spawn failures bubble as wrapped errors when fallback is disabled.
- When fallback is enabled, process startup failures transition to local coordinator mode.
- Start/stop timeout returns `ErrTimeout` and may force-kill process.
- `Health` fails if process is dead or TCP listener is not accepting connections.

### Observability and Debugging
- `PGProcess` directs subprocess stdout/stderr to current process output (`os.Stdout`/`os.Stderr`).
- Main decision points:
  - `coordinator.go:Start`
  - `pgprocess.go:InitDB`
  - `pgprocess.go:Start`
- Tests:
  - `internal/query/sql/coordinator_test.go`
  - `internal/query/sql/pgprocess_test.go`
  - `internal/query/sql/protocol_smoke_test.go`

### Risks and Notes
- Process mode currently depends on local availability of `postgres` and `initdb` binaries.
- Fallback mode keeps runtime available but may hide process-mode startup regressions if not monitored.
- HBA and postgres config are generated from defaults plus raw `ExtraConf` string map without schema validation.

Changes:
