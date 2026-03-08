package observability

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
)

type MetricDescriptor struct {
	Name   string
	Type   string
	Help   string
	Labels []string
}

type HealthStatus struct {
	Component string
	Healthy   bool
	Details   string
}

type metricKey struct {
	Name   string
	Labels string
}

// LogContext carries identity fields attached to every log record.
type LogContext struct {
	NodeID    string
	TabletID  string
	TraceID   string
	RequestID string
}

// histogramData holds per-bucket counts for a histogram metric.
type histogramData struct {
	buckets []float64 // upper bounds
	counts  []uint64  // count per bucket (cumulative)
	sum     float64
	count   uint64
}

// DefaultHistogramBuckets are the default latency bucket boundaries in milliseconds.
var DefaultHistogramBuckets = []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

type Registry struct {
	mu          sync.RWMutex
	descriptors map[string]MetricDescriptor
	values      map[metricKey]float64
	histograms  map[string]*histogramData
	health      map[string]HealthStatus
	logCtx      LogContext
	logger      *slog.Logger
	draining    bool
}

func NewRegistry() *Registry {
	return &Registry{
		descriptors: make(map[string]MetricDescriptor),
		values:      make(map[metricKey]float64),
		histograms:  make(map[string]*histogramData),
		health:      make(map[string]HealthStatus),
		logger:      slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

// SetLogContext attaches node-identity fields to all future log records.
func (r *Registry) SetLogContext(lc LogContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logCtx = lc
}

// Log emits a structured log record at the given level, adding logCtx fields.
func (r *Registry) Log(level slog.Level, msg string, args ...any) {
	r.mu.RLock()
	lc := r.logCtx
	r.mu.RUnlock()
	r.logger.Log(nil, level, msg, append(args,
		"node_id", lc.NodeID,
		"tablet_id", lc.TabletID,
		"trace_id", lc.TraceID,
		"request_id", lc.RequestID,
	)...)
}

// RegisterHistogram registers a histogram metric with the given bucket boundaries.
// If buckets is nil, DefaultHistogramBuckets is used.
func (r *Registry) RegisterHistogram(_ context.Context, name, help string, buckets []float64) error {
	if name == "" {
		return fmt.Errorf("histogram name is required")
	}
	if buckets == nil {
		buckets = DefaultHistogramBuckets
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.histograms[name]; ok {
		return nil // idempotent
	}
	sorted := append([]float64(nil), buckets...)
	sort.Float64s(sorted)
	r.histograms[name] = &histogramData{
		buckets: sorted,
		counts:  make([]uint64, len(sorted)),
	}
	r.descriptors[name] = MetricDescriptor{Name: name, Type: "histogram", Help: help}
	return nil
}

// ObserveHistogram records a single observation in the histogram.
func (r *Registry) ObserveHistogram(_ context.Context, name string, value float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.histograms[name]
	if !ok {
		return fmt.Errorf("histogram not registered: %s", name)
	}
	for i, upper := range h.buckets {
		if value <= upper {
			h.counts[i]++
		}
	}
	h.sum += value
	h.count++
	return nil
}

// HistogramSnapshot returns a snapshot of a histogram's data.
// Returns (buckets, counts, sum, count, ok).
func (r *Registry) HistogramSnapshot(name string) ([]float64, []uint64, float64, uint64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.histograms[name]
	if !ok {
		return nil, nil, 0, 0, false
	}
	buckets := append([]float64(nil), h.buckets...)
	counts := append([]uint64(nil), h.counts...)
	return buckets, counts, h.sum, h.count, true
}

func (r *Registry) RegisterMetric(ctx context.Context, desc MetricDescriptor) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if desc.Name == "" || desc.Type == "" {
		return fmt.Errorf("metric name and type are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.descriptors[desc.Name]; ok {
		return nil
	}
	r.descriptors[desc.Name] = desc
	return nil
}

func (r *Registry) RecordMetric(ctx context.Context, name string, value float64, labels ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if name == "" {
		return fmt.Errorf("metric name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.descriptors[name]; !ok {
		return fmt.Errorf("metric not registered: %s", name)
	}
	k := metricKey{Name: name, Labels: joinLabels(labels)}
	r.values[k] = value
	return nil
}

func (r *Registry) SetHealth(ctx context.Context, hs HealthStatus) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if hs.Component == "" {
		return fmt.Errorf("component is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health[hs.Component] = hs
	return nil
}

func (r *Registry) Healthz(ctx context.Context) ([]HealthStatus, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]HealthStatus, 0, len(r.health))
	for _, v := range r.health {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Component < out[j].Component })
	return out, nil
}

func (r *Registry) Snapshot(ctx context.Context) (map[string]float64, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]float64, len(r.values))
	for k, v := range r.values {
		out[k.Name+"{"+k.Labels+"}"] = v
	}
	return out, nil
}

func joinLabels(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	out := labels[0]
	for i := 1; i < len(labels); i++ {
		out += "," + labels[i]
	}
	return out
}
