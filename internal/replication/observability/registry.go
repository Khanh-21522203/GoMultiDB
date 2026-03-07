package observability

import (
	"context"
	"fmt"
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

type Registry struct {
	mu          sync.RWMutex
	descriptors map[string]MetricDescriptor
	values      map[metricKey]float64
	health      map[string]HealthStatus
}

func NewRegistry() *Registry {
	return &Registry{
		descriptors: make(map[string]MetricDescriptor),
		values:      make(map[metricKey]float64),
		health:      make(map[string]HealthStatus),
	}
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
