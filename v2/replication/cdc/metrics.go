package cdc

import (
	"sync"
	"time"

	"GoMultiDB/v2/replication/observability"
)

// StreamMetrics holds point-in-time CDC stream metrics for a single
// (stream, tablet) pair.
type StreamMetrics struct {
	// ReplicationLagMs is the current lag from source to target, in
	// milliseconds.
	ReplicationLagMs float64
	// RecordsAppliedTotal is the cumulative count of records applied.
	RecordsAppliedTotal uint64
	// ApplyErrorsTotal is the cumulative count of apply errors.
	ApplyErrorsTotal uint64
	// CheckpointStalenessMs is the time since the checkpoint last
	// advanced, in milliseconds.
	CheckpointStalenessMs float64
}

// MetricsRegistry tracks per-stream, per-tablet StreamMetrics and
// publishes them into an observability.Registry.
//
// Scaffold stub: all methods are unimplemented.
type MetricsRegistry struct {
	mu      sync.RWMutex
	metrics map[string]map[string]*StreamMetrics
	regs    *observability.Registry
}

// NewMetricsRegistry returns a MetricsRegistry that publishes into regs.
func NewMetricsRegistry(regs *observability.Registry) *MetricsRegistry {
	return &MetricsRegistry{}
}

// RegisterStream ensures a StreamMetrics entry exists for streamID on
// tabletID.
//
// Not yet implemented in this scaffold.
func (mr *MetricsRegistry) RegisterStream(streamID, tabletID string) {
	panic("not implemented")
}

// RecordApplied increments the applied-record count for streamID on
// tabletID by count and updates its replication lag to lag.
//
// Not yet implemented in this scaffold.
func (mr *MetricsRegistry) RecordApplied(streamID, tabletID string, count uint64, lag time.Duration) {
	panic("not implemented")
}

// RecordError increments the apply-error count for streamID on tabletID.
//
// Not yet implemented in this scaffold.
func (mr *MetricsRegistry) RecordError(streamID, tabletID string) {
	panic("not implemented")
}

// TickStaleness advances the checkpoint-staleness clock for every tracked
// stream and tablet by delta. Intended to be called periodically.
//
// Not yet implemented in this scaffold.
func (mr *MetricsRegistry) TickStaleness(delta time.Duration) {
	panic("not implemented")
}

// GetSnapshot returns the current StreamMetrics for streamID on tabletID,
// and whether an entry exists.
//
// Not yet implemented in this scaffold.
func (mr *MetricsRegistry) GetSnapshot(streamID, tabletID string) (StreamMetrics, bool) {
	panic("not implemented")
}

// GetStreamSnapshots returns the current StreamMetrics for every tablet
// tracked under streamID, keyed by tablet ID.
//
// Not yet implemented in this scaffold.
func (mr *MetricsRegistry) GetStreamSnapshots(streamID string) map[string]StreamMetrics {
	panic("not implemented")
}

// DeleteStream removes all tracked metrics for streamID.
//
// Not yet implemented in this scaffold.
func (mr *MetricsRegistry) DeleteStream(streamID string) {
	panic("not implemented")
}
