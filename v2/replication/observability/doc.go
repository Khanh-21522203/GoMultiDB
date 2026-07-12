// Package observability provides the shared metrics registry, health
// tracking, structured logging context, and HTTP admin handlers used by the
// GoMultiDB v2 replication subsystem: Registry accumulates counters,
// gauges, and histograms and reports per-component health, while the
// admin handlers expose that state as Prometheus-style metrics text,
// JSON varz/rpcz endpoints, pprof profiling routes, and drain/compact/
// cancel operator actions. It is consumed by v2/replication/cdc,
// v2/replication/xcluster, and v2/replication/controlplane for metrics
// registration, health reporting, and admin HTTP surfaces.
// This is scaffold-only; behavior is unimplemented.
package observability
