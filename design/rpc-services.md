# RPC Services

### Purpose
Provides the in-cluster HTTP JSON-RPC transport (`/rpc`) and the baseline `ping.echo` RPC service used for connectivity and contract checks.

### Scope
**In scope:**
- RPC server/client encoding/dispatch in `internal/rpc`.
- Service registration contract (`Service.Name`, `Service.Methods`).
- Ping service request/response contract in `internal/services/ping/service.go`.

**Out of scope:**
- Domain-specific master/tablet/query RPC method implementations.

### Primary User Flow
1. Caller creates `rpc.Client` with base URL.
2. Caller sends envelope + service + method + payload through `Client.Call`.
3. Server validates contract version, resolves service method, executes handler, and returns payload or structured `DBError`.

### System Flow
1. Entry point: `internal/rpc/server.go:handleRPC` on `POST /rpc`.
2. Decode `rpcRequest` (`Envelope`, `Service`, `Method`, `Payload`).
3. Enforce contract version via `versioning.ValidateContractVersion`.
4. Lookup registered service in `Server.services` and method in `Service.Methods()`.
5. Invoke handler and wrap any error through `dberrors.NormalizeError` in `rpcResponse.Error`.
6. Client side (`internal/rpc/client.go:Call`) decodes `rpcResponse` and returns `DBError` as Go error.

### Data Model
- RPC request envelope (`types.RequestEnvelope`):
  - `RequestID (ids.RequestID)`, `SourceNode (ids.NodeID)`, `SentAt (time.Time)`, `ContractVer (uint32)`.
- Transport payload wrappers:
  - `rpcRequest {Envelope, Service, Method, Payload}`.
  - `rpcResponse {Payload, Error *DBError}`.
- Ping models:
  - `ping.Request {Message string}`.
  - `ping.Response {Message, ServerAt, Source, RequestID}`.
- Persistence: none; transport state is in-memory registration map.

### Interfaces and Contracts
- `Server.RegisterService(svc Service) error`:
  - Rejects nil service, empty name, duplicate names.
- `Server.Start(ctx)` binds TCP listener and serves `POST /rpc`.
- `Client.Call(ctx, envelope, service, method, payload)`:
  - Returns retryable unavailable on transport errors.
  - Returns typed DB error when server sets `rpcResponse.Error`.
- Ping service method:
  - `service=ping`, `method=echo`.
  - Empty message is normalized to `"pong"`.

### Dependencies
**Internal modules:**
- `internal/common/errors` for structured error codes.
- `internal/common/types` and `internal/common/versioning` for contract metadata.

**External services/libraries:**
- `net/http`, `encoding/json`, optional TLS config in both server and client transport.

### Failure Modes and Edge Cases
- Non-`POST` requests return `405`.
- Malformed JSON requests return `ErrInvalidArgument` via `rpcResponse.Error`.
- Version mismatch under strict mode returns `ErrInvalidArgument`.
- Unknown service or method returns `ErrInvalidArgument`.
- Client transport failure maps to `ErrRetryableUnavailable`.
- Ping payload unmarshal failures return `ErrInvalidArgument`; response marshal failures return `ErrInternal`.

### Observability and Debugging
- Main debugging points:
  - `internal/rpc/server.go:handleRPC`
  - `internal/rpc/client.go:Call`
  - `internal/services/ping/service.go:echo`
- Integration contract test: `internal/rpc/phase0_integration_test.go` validates ping round-trip and contract version rejection.

### Risks and Notes
- Transport contract is single endpoint (`/rpc`) and JSON-encoded payload bytes; no per-method HTTP path separation.
- No built-in request metrics in `internal/rpc`; observability currently comes from higher-level components/tests.

Changes:

