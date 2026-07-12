package cdc

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// Metrics accumulates cumulative operation counts for a Store.
type Metrics struct {
	PollRequests       uint64
	PollReturnedEvents uint64
	CheckpointAdvances uint64
	CheckpointNoops    uint64
	AppendEvents       uint64
	AppendNoops        uint64
}

// Status summarizes a Store's current size and cumulative metrics.
type Status struct {
	Streams     int
	Tablets     int
	Metrics     Metrics
	LastUpdated time.Time
}

// LagSnapshot reports the replication lag, in events, between the latest
// appended sequence and the current checkpoint for a (stream, tablet)
// pair.
type LagSnapshot struct {
	StreamID    string
	TabletID    string
	LatestSeq   uint64
	Checkpoint  uint64
	LagEvents   uint64
	CollectedAt time.Time
}

// Store is an in-memory, per-stream, per-tablet log of CDC Events and
// their Checkpoints.
//
// Scaffold stub: all methods are unimplemented.
type Store struct {
	mu          sync.RWMutex
	events      map[string]map[string][]Event
	checkpoints map[string]map[string]Checkpoint
	metrics     Metrics
	lastUpdated time.Time
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{}
}

// AppendEvent appends ev to its stream/tablet's event log. Sequence
// numbers must be monotonic per (stream, tablet); a repeat of the last
// sequence is a no-op.
//
// Not yet implemented in this scaffold.
func (s *Store) AppendEvent(ctx context.Context, ev Event) error {
	return dberrors.ErrNotImplemented
}

// Poll returns events for req's stream/tablet after req.AfterSeq, up to
// req.MaxRecords.
//
// Not yet implemented in this scaffold.
func (s *Store) Poll(ctx context.Context, req PollRequest) (PollResponse, error) {
	return PollResponse{}, dberrors.ErrNotImplemented
}

// AdvanceCheckpoint persists cp, rejecting any sequence regression for
// the same (stream, tablet) pair.
//
// Not yet implemented in this scaffold.
func (s *Store) AdvanceCheckpoint(ctx context.Context, cp Checkpoint) error {
	return dberrors.ErrNotImplemented
}

// GetCheckpoint returns the current Checkpoint for (streamID, tabletID),
// or a zero-sequence Checkpoint if none has been recorded.
//
// Not yet implemented in this scaffold.
func (s *Store) GetCheckpoint(ctx context.Context, streamID, tabletID string) (Checkpoint, error) {
	return Checkpoint{}, dberrors.ErrNotImplemented
}

// Streams returns the IDs of all streams with at least one event,
// sorted.
//
// Not yet implemented in this scaffold.
func (s *Store) Streams(ctx context.Context) ([]string, error) {
	return nil, dberrors.ErrNotImplemented
}

// Status returns a summary of the Store's current size and cumulative
// metrics.
//
// Not yet implemented in this scaffold.
func (s *Store) Status(ctx context.Context) (Status, error) {
	return Status{}, dberrors.ErrNotImplemented
}

// LagSnapshot returns the current replication lag for (streamID,
// tabletID).
//
// Not yet implemented in this scaffold.
func (s *Store) LagSnapshot(ctx context.Context, streamID, tabletID string) (LagSnapshot, error) {
	return LagSnapshot{}, dberrors.ErrNotImplemented
}
