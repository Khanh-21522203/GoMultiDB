# Master Catalog Heartbeat

### Purpose
Tracks authoritative table/tablet metadata, enforces leader-only catalog mutations with request dedupe, and reconciles tablet-server heartbeat reports into placement directives.

### Scope
**In scope:**
- Catalog manager state machine and mutation APIs in `internal/master/catalog`.
- Durable catalog persistence in `internal/master/syscatalog/store.go`.
- Heartbeat request handling and action planning in `internal/master/heartbeat/service.go`.
- Tablet endpoint resolution registry in `internal/master/registry/tablet_registry.go`.

**Out of scope:**
- Distributed snapshot execution (covered in snapshot feature).

### Primary User Flow
1. Master leader receives tserver heartbeat with instance/registration/report data.
2. Master updates tserver descriptor and reconciles reported tablet placements.
3. Planner computes create/delete tablet directives for under/over replication.
4. Catalog APIs (`CreateTable`, `AlterTable`, `DeleteTable`, `CreateTablet`) mutate metadata with idempotent request semantics.

### System Flow
1. Heartbeat entry: `heartbeat.Service.TSHeartbeat`.
2. Validates leader status + tserver identity and sequence ordering.
3. Applies incremental/full tablet report via `catalog.Manager.ApplyTabletReport` to `MemoryReconcileSink`.
4. `buildActionsResponse` filters stale tservers, evaluates each changed tablet through `DirectivePlanner.PlanForTablet`, and returns `TabletActions`.
5. Catalog mutation path uses `CatalogStore.Apply` and reloads snapshot (`LoadSnapshot`) after every accepted mutation.

```
TSHeartbeat
  ├── TSManager descriptor update
  ├── Catalog reconcile sink update
  └── DirectivePlanner -> CREATE_TABLET / DELETE_TABLET actions
```

### Data Model
- `TableInfo`:
  - `TableID`, `NamespaceID`, `Name`, `State`, `Version`, `Epoch`, `CreateReqID`.
- `TabletInfo`:
  - `TabletID`, `TableID`, `NamespaceID`, `State`, `ReplicaCount`.
- `CatalogMutation`:
  - `RequestID`, `RequestKind`, `RequestFingerprint`, `RequestValue`, `UpsertTable`, `UpsertTablet`.
- Heartbeat models:
  - `TSInstance`, `TSRegistration`, `TabletReport`, `HeartbeatRequest`, `HeartbeatResponse`.
- Persistence keys (`SysCatalogStore` RegularDB):
  - `entity/table/<table_id>` -> JSON `TableInfo`.
  - `entity/tablet/<tablet_id>` -> JSON `TabletInfo`.
  - `reqlog/<request_id>` -> dedupe payload (`kind|fingerprint|value`).

### Interfaces and Contracts
- Catalog contracts:
  - `CreateTable`, `AlterTable`, `DeleteTable`, `CreateTablet` require leader mode.
  - Duplicate `RequestID` with different fingerprint returns `ErrIdempotencyConflict`.
- Heartbeat RPC contract:
  - Service `heartbeat`, method `ts_heartbeat`.
  - Returns `NeedReregister` and `NeedFullTabletReport` when state is out-of-sync.
- Registry contract:
  - `TabletRPCRegistry.GetEndpoint(tabletID)` resolves `http://<ts-rpc-address>` from reconcile + tserver metadata.

### Dependencies
**Internal modules:**
- `internal/common/errors` for canonical failure codes.
- `internal/master/catalog` used by heartbeat for reconciliation/planning.
- `internal/storage/rocks` through `SysCatalogStore`.

**External services/libraries:**
- None; data is in-process or persisted through internal Rocks abstraction.

### Failure Modes and Edge Cases
- Non-leader catalog or heartbeat operations return `ErrNotLeader` (retryable).
- Catalog state transition violations return `ErrConflict`.
- Duplicate request ID with changed request shape returns `ErrIdempotencyConflict`.
- Stale or missing incremental report sequences force `NeedFullTabletReport`.
- Registry endpoint lookup errors:
  - unknown tablet -> `ErrInvalidArgument`
  - tombstoned tablet -> `ErrConflict`
  - no live replica -> `ErrRetryableUnavailable`

### Observability and Debugging
- Primary debugging points:
  - `catalog/manager.go` mutation and dedupe path
  - `heartbeat/service.go:TSHeartbeat`
  - `catalog/directives.go:PlanForTablet`
- Coverage files:
  - `catalog/*_test.go`
  - `heartbeat/service_test.go`
  - `registry/tablet_registry_test.go`
  - `syscatalog/store_test.go`

### Risks and Notes
- `MemoryReconcileSink` is in-memory; reconcile view is rebuilt from heartbeat traffic, not durable state.
- Registry leader selection is currently “first replica in map iteration”, not raft-leader-aware.

Changes:

