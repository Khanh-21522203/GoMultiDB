# SQL Control

### Purpose
Provides SQL process lifecycle abstractions: a lightweight local coordinator used by runtime and a `PGProcess` wrapper for managing an external postgres-compatible subprocess.

### Scope
**In scope:**
- `LocalCoordinator` start/stop/health contract in `internal/query/sql/coordinator.go`.
- `PGProcess` init/start/stop/health and config file generation in `pgprocess.go`.
- Catalog version tracker used for cache invalidation in `pgprocess.go`.

**Out of scope:**
- SQL query parsing/plan execution.
- Master catalog/table metadata semantics.

### Primary User Flow
1. Runtime starts SQL path with `ProcessConfig`.
2. Coordinator validates config and reports health for status checks.
3. (Optional real process path) `PGProcess` initializes data dir, writes config, starts `postgres` and probes readiness.

### System Flow
1. Entry point from runtime: `internal/server/runtime.go:Start` calls `sqlCoord.Start(ctx, ProcessConfig{...})`.
2. `LocalCoordinator.Start` enforces required bind address and default max connections when enabled.
3. `PGProcess.Start` path:
   - `InitDB` (creates data dir if missing, runs `initdb`).
   - Writes `postgresql.conf` and `pg_hba.conf`.
   - Launches `postgres` process and polls TCP readiness.
4. `Stop` sends interrupt and force-kills on timeout.

### Data Model
- `ProcessConfig`:
  - `Enabled`, `BindAddress`, `MaxConnections`, `HBAConfig`, `ExtraConf`.
- `PGProcessConfig`:
  - `DataDir`, `BindAddress`, `Port`, `BinPath`, `InitDBPath`, `StartTimeout`, `StopTimeout`, `HBAConfigContent`, `ExtraConf`.
- `CatalogVersion` tracker:
  - `version (uint64)` with `Get/Increment/Set`.
- Persistence:
  - Process config files are written to `DataDir/postgresql.conf` and `DataDir/pg_hba.conf`.

### Interfaces and Contracts
- `Coordinator` interface: `Start`, `Stop`, `Health`.
- `LocalCoordinator.Start` idempotent only for same config; differing second start returns `ErrConflict`.
- `Health` returns `ErrRetryableUnavailable` if not started.
- `PGProcess` contract:
  - Requires `DataDir`.
  - Defaults to `127.0.0.1:5433`, start timeout 30s, stop timeout 10s.

### Dependencies
**Internal modules:**
- `internal/common/errors` for typed error codes.

**External services/libraries:**
- `os/exec` for running `initdb` and `postgres` binaries.
- TCP probe via `net.DialTimeout`.

### Failure Modes and Edge Cases
- Missing bind address when enabled -> `ErrInvalidArgument`.
- Start with different config while started -> `ErrConflict`.
- `initdb` or postgres spawn failures bubble as wrapped errors.
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
- Runtime currently instantiates `NewLocalCoordinator`; `PGProcess` is implemented but not wired in runtime startup path.
- HBA and postgres config are generated from defaults plus raw `ExtraConf` string map without schema validation.

Changes:

- Finish SQL surface so runtime uses a complete SQL path rather than coordinator-only scaffolding.
- Complete integration for real process and execution flow, including production-ready startup, health, and query-serving behavior.
