package cdc

import (
	"context"
	"sync"
	"time"

	"GoMultiDB/internal/replication/observability"
)

// StreamMetrics holds CDC stream metrics.
type StreamMetrics struct {
	ReplicationLagMs     float64 // current lag from source to target
	RecordsAppliedTotal  uint64  // cumulative records applied
	ApplyErrorsTotal     uint64  // cumulative apply errors
	CheckpointStalenessMs float64 // time since last checkpoint advanced
}

// MetricsRegistry tracks per-stream metrics.
type MetricsRegistry struct {
	mu      sync.RWMutex
	metrics map[string]map[string]*StreamMetrics // streamID -> tabletID -> StreamMetrics
	regs    *observability.Registry
}

// NewMetricsRegistry creates a MetricsRegistry.
func NewMetricsRegistry(regs *observability.Registry) *MetricsRegistry {
	return &MetricsRegistry{
		metrics: make(map[string]map[string]*StreamMetrics),
		regs:    regs,
	}
}

// RegisterStream ensures metrics entries exist for a stream on a tablet.
func (mr *MetricsRegistry) RegisterStream(streamID, tabletID string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if mr.metrics[streamID] == nil {
		mr.metrics[streamID] = make(map[string]*StreamMetrics)
	}
	if _, ok := mr.metrics[streamID][tabletID]; !ok {
		mr.metrics[streamID][tabletID] = &StreamMetrics{}
	}
}

// RecordApplied increments the applied count and updates lag.
func (mr *MetricsRegistry) RecordApplied(streamID, tabletID string, count uint64, lag time.Duration) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.ensureExists(streamID, tabletID)
	m := mr.metrics[streamID][tabletID]
	m.RecordsAppliedTotal += count
	m.ReplicationLagMs = float64(lag.Milliseconds())
	m.CheckpointStalenessMs = 0 // just advanced
	mr.publishGauges(streamID, tabletID, m)
}

// RecordError increments the error count for a stream.
func (mr *MetricsRegistry) RecordError(streamID, tabletID string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.ensureExists(streamID, tabletID)
	m := mr.metrics[streamID][tabletID]
	m.ApplyErrorsTotal++
	mr.publishGauges(streamID, tabletID, m)
}

// TickStaleness updates staleness for all streams (called periodically).
func (mr *MetricsRegistry) TickStaleness(delta time.Duration) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	for streamID, tablets := range mr.metrics {
		for tabletID, m := range tablets {
			m.CheckpointStalenessMs += float64(delta.Milliseconds())
			mr.publishGauges(streamID, tabletID, m)
		}
	}
}

// GetSnapshot returns a snapshot of metrics for a stream on a tablet.
func (mr *MetricsRegistry) GetSnapshot(streamID, tabletID string) (StreamMetrics, bool) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	if mr.metrics[streamID] == nil {
		return StreamMetrics{}, false
	}
	m, ok := mr.metrics[streamID][tabletID]
	if !ok {
		return StreamMetrics{}, false
	}
	return *m, true
}

// GetStreamSnapshots returns snapshots for all tablets in a stream.
func (mr *MetricsRegistry) GetStreamSnapshots(streamID string) map[string]StreamMetrics {
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	out := make(map[string]StreamMetrics, len(mr.metrics[streamID]))
	for tabletID, m := range mr.metrics[streamID] {
		out[tabletID] = *m
	}
	return out
}

// DeleteStream removes all metrics for a stream.
func (mr *MetricsRegistry) DeleteStream(streamID string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	delete(mr.metrics, streamID)
}

func (mr *MetricsRegistry) ensureExists(streamID, tabletID string) {
	if mr.metrics[streamID] == nil {
		mr.metrics[streamID] = make(map[string]*StreamMetrics)
	}
	if mr.metrics[streamID][tabletID] == nil {
		mr.metrics[streamID][tabletID] = &StreamMetrics{}
	}
}

func (mr *MetricsRegistry) publishGauges(streamID, tabletID string, m *StreamMetrics) {
	if mr.regs == nil {
		return
	}
	ctx := context.Background()
	// Incorporate IDs into histogram name since Registry doesn't support labels.
	lagName := "cdc_replication_lag_ms/" + streamID + "/" + tabletID
	staleName := "cdc_checkpoint_staleness_ms/" + streamID + "/" + tabletID
	_ = mr.regs.ObserveHistogram(ctx, lagName, float64(m.ReplicationLagMs))
	_ = mr.regs.ObserveHistogram(ctx, staleName, float64(m.CheckpointStalenessMs))
}
