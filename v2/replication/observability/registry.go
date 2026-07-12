package observability

import (
	"context"
	"log/slog"
	"sync"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// MetricDescriptor describes a single registered metric: its name, kind
// ("counter", "gauge", or "histogram"), help text, and label names.
type MetricDescriptor struct {
	Name   string
	Type   string
	Help   string
	Labels []string
}

// HealthStatus reports the health of a single named component.
type HealthStatus struct {
	Component string
	Healthy   bool
	Details   string
}

// LogContext carries identity fields attached to every log record emitted
// through Registry.Log.
type LogContext struct {
	NodeID    string
	TabletID  string
	TraceID   string
	RequestID string
}

// DefaultHistogramBuckets are the default latency bucket boundaries, in
// milliseconds, used by RegisterHistogram when no explicit buckets are
// supplied.
var DefaultHistogramBuckets = []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

// Registry accumulates metrics, health statuses, and logging context for a
// single node, and renders them as Prometheus-style text and JSON via the
// handlers in admin.go.
//
// Scaffold stub: all methods are unimplemented.
type Registry struct {
	mu          sync.RWMutex
	descriptors map[string]MetricDescriptor
	values      map[string]float64
	health      map[string]HealthStatus
	logCtx      LogContext
	logger      *slog.Logger
	draining    bool
}

// NewRegistry returns an empty Registry ready to accept metric and health
// registrations.
func NewRegistry() *Registry {
	return &Registry{}
}

// SetLogContext attaches node-identity fields to all future log records
// emitted through Log.
//
// Not yet implemented in this scaffold.
func (r *Registry) SetLogContext(lc LogContext) {
	panic("not implemented")
}

// Log emits a structured log record at the given level, enriched with the
// fields from the current LogContext.
//
// Not yet implemented in this scaffold.
func (r *Registry) Log(level slog.Level, msg string, args ...any) {
	panic("not implemented")
}

// RegisterHistogram registers a histogram metric with the given bucket
// boundaries. If buckets is nil, DefaultHistogramBuckets is used.
// Registration is idempotent.
//
// Not yet implemented in this scaffold.
func (r *Registry) RegisterHistogram(ctx context.Context, name, help string, buckets []float64) error {
	return dberrors.ErrNotImplemented
}

// ObserveHistogram records a single observation against a previously
// registered histogram.
//
// Not yet implemented in this scaffold.
func (r *Registry) ObserveHistogram(ctx context.Context, name string, value float64) error {
	return dberrors.ErrNotImplemented
}

// HistogramSnapshot returns a histogram's current bucket upper bounds,
// per-bucket cumulative counts, running sum, and total count. The final
// bool reports whether name is a registered histogram.
//
// Not yet implemented in this scaffold.
func (r *Registry) HistogramSnapshot(name string) ([]float64, []uint64, float64, uint64, bool) {
	panic("not implemented")
}

// RegisterMetric registers a counter or gauge metric described by desc.
// Registration is idempotent.
//
// Not yet implemented in this scaffold.
func (r *Registry) RegisterMetric(ctx context.Context, desc MetricDescriptor) error {
	return dberrors.ErrNotImplemented
}

// RecordMetric sets the current value of a previously registered counter
// or gauge metric, optionally qualified by label values.
//
// Not yet implemented in this scaffold.
func (r *Registry) RecordMetric(ctx context.Context, name string, value float64, labels ...string) error {
	return dberrors.ErrNotImplemented
}

// SetHealth records the current HealthStatus of a named component.
//
// Not yet implemented in this scaffold.
func (r *Registry) SetHealth(ctx context.Context, hs HealthStatus) error {
	return dberrors.ErrNotImplemented
}

// RecordPrimaryOwnershipTransition records a primary-ownership transfer
// event for a stream/tablet pair, used by post-failover observability
// surfaces.
//
// Not yet implemented in this scaffold.
func (r *Registry) RecordPrimaryOwnershipTransition(ctx context.Context, streamID, tabletID, fromNode, toNode string, epoch uint64) error {
	return dberrors.ErrNotImplemented
}

// Healthz returns the HealthStatus of every registered component, sorted
// by component name.
//
// Not yet implemented in this scaffold.
func (r *Registry) Healthz(ctx context.Context) ([]HealthStatus, error) {
	return nil, dberrors.ErrNotImplemented
}

// Snapshot returns the current value of every recorded counter and gauge
// metric, keyed by "<name>{<labels>}".
//
// Not yet implemented in this scaffold.
func (r *Registry) Snapshot(ctx context.Context) (map[string]float64, error) {
	return nil, dberrors.ErrNotImplemented
}
