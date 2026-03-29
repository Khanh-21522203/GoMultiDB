# CQL Gateway

### Purpose
Implements a local CQL-compatible gateway with binary frame parsing, connection/session state, prepared statement cache behavior, and a dedicated in-package execution backend for core CQL DML operations.

### Scope
**In scope:**
- CQL listener/socket lifecycle in `internal/query/cql/listener.go`.
- Protocol frame and message codecs in `protocol.go` and `messages.go`.
- Session and prepared statement cache behavior in `session.go`.
- Server lifecycle and connection tracking in `server.go`.
- Core execution engine and statement parsing in `executor.go`.

**Out of scope:**
- Full CQL surface parity (current execution backend handles core SELECT/INSERT/UPDATE/DELETE).
- Auth/event subscription protocol workflows (returned as explicit unsupported errors).

### Primary User Flow
1. Client opens TCP connection to CQL bind address.
2. Client sends startup/options/query/prepare/execute frames.
3. Listener parses frames and dispatches to `LocalServer` + `SessionManager`.
4. Server executes core DML via local execution engine and returns READY/SUPPORTED/RESULT/ERROR frames.

### System Flow
1. Entry: `internal/query/cql/listener.go:Start` binds listener when `Config.Enabled`.
2. `acceptLoop` creates `Connection` wrapper and opens server session by `conn.RemoteAddr().String()`.
3. `handleFrame` switches by opcode:
   - `OpcodeStartup` -> READY
   - `OpcodeOptions` -> SUPPORTED
   - `OpcodeQuery` -> `LocalServer.Route`
   - `OpcodePrepare` -> `SessionManager.Prepare`
   - `OpcodeExecute` -> prepared lookup + `LocalServer.Route`
   - `OpcodeBatch` -> `BatchRequest.Unmarshal` + `LocalServer.RouteBatch`
   - `OpcodeRegister` and auth opcodes -> explicit protocol ERROR (unsupported)
4. `LocalServer` maintains active connection limits, resolves prepared statements, and executes core DML through `executionEngine`.

```
TCP Conn
  └── ReadFrame
        └── handleFrame(opcode)
              ├── prepare -> session cache insert
              ├── execute -> prepared resolve + executionEngine
              ├── batch   -> per-item Route dispatch
              └── query   -> executionEngine
```

### Data Model
- `Config {Enabled, BindAddress, MaxConnections}`.
- `Request {ConnID, Query, PreparedID, Vars}` and `Response {Applied, Rows}`.
- `Session` fields:
  - `ConnID`, `Prepared map[string]PreparedStmt`, `Keyspace`, `Consistency`, `SchemaVer`.
- `PreparedStmt`:
  - `ID`, `Query`, `Plan []byte`, `SchemaVer`.
- `PreparedStats`:
  - `CacheHits`, `CacheMisses`, `InvalidationCount`.
- `executionEngine`:
  - In-memory per-table row counters keyed by parsed table identifier.
  - Statement parser for core CQL forms (`SELECT`, `INSERT`, `UPDATE`, `DELETE`).
- Persistence: none; all gateway/session state is in-memory.

### Interfaces and Contracts
- `LocalServer.Start/Stop/Health/Route/RouteBatch` compose the public server interface.
- `Listener` contract:
  - Must be started with non-empty bind address.
  - Returns protocol `ERROR` frame for parse/unsupported opcodes.
- CQL protocol contracts represented by:
  - `Frame` (header + body), `Opcode*` constants.
  - Message models in `messages.go` (`QueryRequest`, `ExecuteRequest`, `ResultResponse`, etc.).

### Dependencies
**Internal modules:**
- `internal/common/errors` for validation/conflict/retryable error mapping.

**External services/libraries:**
- `net` sockets and binary encoding utilities in `encoding/binary`.

### Failure Modes and Edge Cases
- Start validation errors: empty bind address, invalid max connection values.
- Max connection limit returns `ErrRetryableUnavailable`.
- Missing connection/session/prepared IDs return `ErrInvalidArgument`.
- Prepared statement schema mismatch returns `ErrConflict` (retryable flag true).
- Unsupported statement classes return `ErrInvalidArgument`.
- Oversized frame read (>256MB) fails in `Connection.ReadFrame`.
- Unsupported register/auth opcodes generate explicit protocol error responses.

### Observability and Debugging
- No dedicated metric/log stream in this package; debugging typically starts at:
  - `listener.go:handleFrame`
  - `server.go:Route`
  - `session.go:ExecutePrepared`
- Behavior coverage:
  - `internal/query/cql/protocol_test.go`
  - `internal/query/cql/protocol_smoke_test.go`
  - `internal/query/cql/server_test.go`
  - `internal/query/cql/session_test.go`

### Risks and Notes
- Execution backend is intentionally scoped to core DML and in-memory semantics; advanced CQL features and storage-backed plans are not implemented yet.
- Auth and event subscription opcodes remain explicitly unsupported.

Changes:
