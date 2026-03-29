# Runtime Bootstrap

### Purpose
Bootstraps `master` and `tserver` processes, wires core services into `server.Runtime`, and applies node config defaults/validation.

### Scope
**In scope:**
- CLI startup paths in `cmd/master/main.go` and `cmd/tserver/main.go`.
- Runtime process lifecycle in `internal/server/runtime.go` (`Init`, `Start`, `Stop`).
- Node config defaults and validation in `internal/server/config.go`.
- Local container runtime wiring in `Dockerfile.*` and `docker-compose*.yml`.

**Out of scope:**
- Request-level RPC method semantics (documented in `rpc-services.md`).
- Master catalog/heartbeat/snapshot business logic (master feature docs).

### Primary User Flow
1. Operator starts `master` or `tserver` binary with optional `-node-id`, `-rpc-addr`, `-http-addr` flags.
2. Process builds `server.Config`, creates an RPC server, and registers role-specific services.
3. Runtime starts RPC listener and query surfaces (`CQL`, optionally `SQL`).
4. On `SIGINT`/`SIGTERM`, runtime performs phased shutdown (query first, then RPC).

### System Flow
1. Entry points: `cmd/master/main.go:main` and `cmd/tserver/main.go:main`.
2. Both create `rpc.Server` via `internal/rpc/server.go:NewServer` and register `ping`.
3. `master` additionally wires `catalog.Manager`, `heartbeat.Service`, `registry.TabletRPCRegistry`, and `snapshot.Service`.
4. `tserver` wires `tablet/snapshot.Service`.
5. `master` explicitly sets local primary ownership (`catalogMgr.SetPrimary(true)`, `heartbeatSvc.SetPrimary(true)`) before runtime start.
6. Both call `internal/server/runtime.go:NewRuntime` or `NewRuntimeWithTabletRPC`, then `Init` and `Start`.
7. Startup boundary order:
   - control-plane service wiring + registration first (master/tserver main),
   - runtime listeners second (`Runtime.Start`: RPC -> SQL -> CQL).
8. `Runtime.Stop` reverses query-listener order then stops RPC.

```
master/tserver main
  └── NewServer + RegisterService(s)
        └── NewRuntime(...)
              ├── Start RPC (/rpc)
              ├── Start SQL coordinator
              └── Start CQL listener
```

### Data Model
- `server.Config` (`internal/server/config.go`) fields include runtime identity and limits:
  - `NodeID (string)`, `RPCBindAddress (string)`, `HTTPBindAddress (string)`.
  - `DataDirs ([]string)`, `WALDirs ([]string)`, `MemoryHardLimitBytes (int64)`.
  - Query toggles: `EnableSQL`, `SQLBindAddress`, `EnableCQL`, `CQLBindAddress`.
  - Snapshot toggles: `EnableSnapshotCoord`, `MaxConcurrentSnaps`.
- Persistence behavior:
  - `Config` itself is not persisted by runtime.
  - Runtime may instantiate snapshot/catalog stores backed by `rocks.Store` for downstream modules.

### Interfaces and Contracts
- CLI contracts:
  - `cmd/master`: `-node-id`, `-rpc-addr`, `-http-addr`.
  - `cmd/tserver`: same flag contract.
- Config contract: `ValidateConfig(cfg)` returns `ErrInvalidConfig` for first invalid field (empty `NodeID`, invalid `host:port`, missing `DataDirs`, WAL/Data overlap, invalid memory/clock bounds).
- Runtime contracts:
  - `Start(ctx)` is idempotent for already-started runtime.
  - `Stop(ctx)` is guarded by `stopped` atomic flag and executes two phases (`ShutdownPhase()` reports 1 then 2).

### Dependencies
**Internal modules:**
- `internal/rpc` for transport.
- `internal/query/sql` and `internal/query/cql` for query listener surfaces.
- `internal/master/*` and `internal/tablet/snapshot` for role-specific service wiring.
- `internal/storage/rocks` as backing store implementation.

**External services/libraries:**
- OS signal handling (`os/signal`, `syscall`) for graceful shutdown.
- Docker runtime via Compose files and Dockerfiles for local deployment.

### Failure Modes and Edge Cases
- `NewRuntimeWithTabletRPC` fails if `NodeID` is empty or `rpcServer` is nil.
- `Start` fails fast if RPC start fails; if SQL or CQL start fails, earlier started components are stopped before return.
- `Stop` returns phase-specific wrapped errors (`shutdown phase 1` or `shutdown phase 2`).
- `ValidateConfig` rejects overlapping `DataDirs` and `WALDirs` after slash normalization.
- `HTTPBindAddress` is configured by CLI/config but no HTTP admin server is started by `Runtime` in current code.

### Observability and Debugging
- Process logs include startup/shutdown events in CLI mains and shutdown phase logs in `Runtime.Stop` via `slog.Info`.
- Debug start path from:
  - `cmd/master/main.go:main`
  - `cmd/tserver/main.go:main`
  - `internal/server/runtime.go:Start`
- Integration coverage includes:
  - `internal/server/runtime_test.go`
  - `internal/server/runtime_snapshot_test.go`
  - `internal/server/phase5_smoke_test.go`
  - `internal/server/phase5_failover_smoke_test.go`

### Risks and Notes
- Runtime currently uses in-memory Rocks store in both binaries (`rocks.NewMemoryStore()`), so persistence is process-lifetime unless binaries are changed.
- `SQL` integration in runtime uses local coordinator abstraction, while real `PGProcess` remains a separate component.

Changes:
