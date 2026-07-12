package xcluster

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/replication/cdc"
)

// RetryPolicy configures how many times, and with what backoff, a Loop
// retries a failed Applier.Apply call.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

// Config configures a Loop.
type Config struct {
	Retry RetryPolicy
}

// Stats accumulates cumulative apply statistics for a Loop.
type Stats struct {
	AppliedEvents   uint64
	DuplicateEvents uint64
	RetryCount      uint64
	FailureCount    uint64
	LastAppliedSeq  uint64
	LastUpdated     time.Time
}

// Status summarizes a Loop's current configuration and statistics.
type Status struct {
	Config Config
	Stats  Stats
}

// Applier applies a single cdc.Event to the target cluster.
type Applier interface {
	Apply(ctx context.Context, ev cdc.Event) error
}

// CheckpointStore persists and retrieves cdc.Checkpoint values for the
// target cluster.
type CheckpointStore interface {
	AdvanceCheckpoint(ctx context.Context, cp cdc.Checkpoint) error
	GetCheckpoint(ctx context.Context, streamID, tabletID string) (cdc.Checkpoint, error)
}

// LagProvider optionally supplements a CheckpointStore with a direct
// cdc.LagSnapshot lookup, bypassing the checkpoint-only lag estimate.
type LagProvider interface {
	LagSnapshot(ctx context.Context, streamID, tabletID string) (cdc.LagSnapshot, error)
}

// Loop consumes cdc.Event batches, deduplicates them against a
// CheckpointStore, applies them through an Applier with retry, and
// tracks Stats. If store also implements LagProvider, LagSnapshot
// delegates to it directly.
//
// Scaffold stub: all methods are unimplemented.
type Loop struct {
	mu sync.RWMutex

	cfg         Config
	store       CheckpointStore
	lagProvider LagProvider
	applier     Applier
	appliedSet  map[string]struct{}
	stats       Stats
}

// NewLoop returns a Loop configured by cfg, applying events through
// applier and tracking progress in store.
func NewLoop(cfg Config, store CheckpointStore, applier Applier) (*Loop, error) {
	return &Loop{}, nil
}

// ApplyBatch applies each event in events in order via ApplyEvent,
// stopping at the first error.
//
// Not yet implemented in this scaffold.
func (l *Loop) ApplyBatch(ctx context.Context, events []cdc.Event) error {
	return dberrors.ErrNotImplemented
}

// ApplyEvent deduplicates and applies a single cdc.Event, advancing the
// checkpoint store on success.
//
// Not yet implemented in this scaffold.
func (l *Loop) ApplyEvent(ctx context.Context, ev cdc.Event) error {
	return dberrors.ErrNotImplemented
}

// ResumeCheckpoint returns the current checkpoint for (streamID,
// tabletID), for resuming a poller after a restart.
//
// Not yet implemented in this scaffold.
func (l *Loop) ResumeCheckpoint(ctx context.Context, streamID, tabletID string) (cdc.Checkpoint, error) {
	return cdc.Checkpoint{}, dberrors.ErrNotImplemented
}

// LagSnapshot returns the current replication lag for (streamID,
// tabletID), preferring the configured LagProvider if one is available.
//
// Not yet implemented in this scaffold.
func (l *Loop) LagSnapshot(ctx context.Context, streamID, tabletID string) (cdc.LagSnapshot, error) {
	return cdc.LagSnapshot{}, dberrors.ErrNotImplemented
}

// Stats returns the Loop's current cumulative statistics.
//
// Not yet implemented in this scaffold.
func (l *Loop) Stats(ctx context.Context) (Stats, error) {
	return Stats{}, dberrors.ErrNotImplemented
}

// Status returns the Loop's current configuration and statistics.
//
// Not yet implemented in this scaffold.
func (l *Loop) Status(ctx context.Context) (Status, error) {
	return Status{}, dberrors.ErrNotImplemented
}
