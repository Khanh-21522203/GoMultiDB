# Replication Observability

### Purpose
Provides metrics and health registry APIs plus HTTP admin endpoints (`/metrics`, varz/rpcz style, pprof, drain, tablet/admin helper handlers).

### Scope
**In scope:**
- Metric descriptors/values/histograms in `internal/replication/observability/registry.go`.
- Structured logging context support.
- HTTP handler surfaces in `internal/replication/observability/admin.go`.

**Out of scope:**
- Domain-specific collection logic in CDC/control-plane/xcluster (they call into this package).

### Primary User Flow
1. Component registers metrics and health statuses.
2. Component records values/histogram observations.
3. Operator scrapes `/metrics` and inspects health/admin endpoints.
4. Operator triggers drain/compaction/cancel hooks via handler endpoints.

### System Flow
1. Registry stores metric descriptors and labeled values in memory.
2. Histogram registration creates fixed upper-bound bucket arrays.
3. `MetricsHandler` renders Prometheus text format from gauge/counter snapshots and histogram cumulative buckets.
4. `VarzHandler` and `RPCzHandler` serialize health snapshots.
5. `DrainHandler` toggles drain mode flag; helper handlers call injected compaction/cancel/tablet-state providers.

### Data Model
- `MetricDescriptor {Name, Type, Help, Labels}`.
- `HealthStatus {Component, Healthy, Details}`.
- Histogram model (`histogramData`):
  - `buckets []float64`, `counts []uint64`, `sum`, `count`.
- Log context model:
  - `LogContext {NodeID, TabletID, TraceID, RequestID}`.
- Persistence: none; registry state is in-memory.

### Interfaces and Contracts
- Metric contracts:
  - `RegisterMetric`, `RecordMetric`, `Snapshot`.
  - `RegisterHistogram`, `ObserveHistogram`, `HistogramSnapshot`.
- Health contracts:
  - `SetHealth`, `Healthz`.
- Admin HTTP contracts:
  - `DrainHandler` requires `POST`.
  - `CompactHandler` requires `tablet_id` query parameter.
  - `CancelHandler` requires `request_id` query parameter.

### Dependencies
**Internal modules:**
- Used by CDC metrics bridge (`internal/replication/cdc/metrics.go`).

**External services/libraries:**
- `net/http` + `net/http/pprof` for handler surfaces.
- `log/slog` for structured logging.

### Failure Modes and Edge Cases
- Registering/recording unnamed metrics returns error.
- Observing unregistered histogram returns error.
- Invalid method on drain endpoint returns `405`.
- Missing required admin query parameters return `400`.

### Observability and Debugging
- This package is the observability surface itself.
- Debug points:
  - `registry.go:RegisterMetric/RecordMetric`
  - `admin.go:MetricsHandler`
- Coverage:
  - `registry_test.go`
  - `observability_new_test.go`

### Risks and Notes
- Metric labels are flattened into strings, and histogram usage in CDC currently encodes dimensions into metric names rather than first-class label vectors.
- Registry has no retention policy; metric cardinality growth is caller-controlled.

Changes:

