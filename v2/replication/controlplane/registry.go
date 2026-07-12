package controlplane

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/replication/cdc"
	"GoMultiDB/v2/replication/xcluster"
)

// StreamState is the lifecycle state of a replication Stream.
type StreamState string

// JobState is the lifecycle state of a replication Job.
type JobState string

// StreamState and JobState values.
const (
	StreamStateRunning StreamState = "RUNNING"
	StreamStatePaused  StreamState = "PAUSED"
	StreamStateStopped StreamState = "STOPPED"

	JobStateRunning JobState = "RUNNING"
	JobStatePaused  JobState = "PAUSED"
	JobStateStopped JobState = "STOPPED"
)

// Stream is a single replication stream's control-plane record: its
// tablet assignment, primary-ownership epoch, lifecycle state, and latest
// observed checkpoint/lag.
type Stream struct {
	ID             string
	TabletID       string
	PrimaryOwner   string
	OwnershipEpoch uint64
	LastFailoverAt time.Time
	State          StreamState
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Checkpoint     uint64
	LagEvents      uint64
}

// Job is a single replication job's control-plane record: the stream it
// serves and the target it replicates to.
type Job struct {
	ID        string
	StreamID  string
	Target    string
	State     JobState
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Snapshot is a point-in-time view of every Stream and Job, together with
// the apply-loop statistics observed at collection time.
type Snapshot struct {
	GeneratedAt time.Time
	Streams     []Stream
	Jobs        []Job
	Apply       xcluster.Stats
}

// Registry tracks the lifecycle of replication Streams and Jobs, and
// optionally persists them to a local file.
//
// Scaffold stub: all methods are unimplemented.
type Registry struct {
	mu      sync.RWMutex
	streams map[string]Stream
	jobs    map[string]Job
	path    string
}

// NewRegistry returns an empty, in-memory-only Registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// NewRegistryWithFile returns a Registry that persists its Streams and
// Jobs to the file at path, loading any existing state from disk.
func NewRegistryWithFile(path string) (*Registry, error) {
	return &Registry{}, nil
}

// CreateStream registers a new Stream over tabletID in
// StreamStateRunning. Idempotent if id already exists.
//
// Not yet implemented in this scaffold.
func (r *Registry) CreateStream(ctx context.Context, id, tabletID string) error {
	return dberrors.ErrNotImplemented
}

// UpdatePrimaryOwnership updates a stream's primary-owner and
// ownership-epoch metadata, used by post-failover schedulers to react to
// ownership transfer events. If ownershipEpoch is zero, it is derived by
// incrementing the stream's current epoch.
//
// Not yet implemented in this scaffold.
func (r *Registry) UpdatePrimaryOwnership(ctx context.Context, id, primaryOwner string, ownershipEpoch uint64, failover bool) error {
	return dberrors.ErrNotImplemented
}

// PauseStream transitions a stream into StreamStatePaused.
//
// Not yet implemented in this scaffold.
func (r *Registry) PauseStream(ctx context.Context, id string) error {
	return dberrors.ErrNotImplemented
}

// ResumeStream transitions a stream into StreamStateRunning.
//
// Not yet implemented in this scaffold.
func (r *Registry) ResumeStream(ctx context.Context, id string) error {
	return dberrors.ErrNotImplemented
}

// StopStream transitions a stream into StreamStateStopped. This
// transition is terminal.
//
// Not yet implemented in this scaffold.
func (r *Registry) StopStream(ctx context.Context, id string) error {
	return dberrors.ErrNotImplemented
}

// CreateJob registers a new Job replicating streamID to target, in
// JobStateRunning. Idempotent if id already exists.
//
// Not yet implemented in this scaffold.
func (r *Registry) CreateJob(ctx context.Context, id, streamID, target string) error {
	return dberrors.ErrNotImplemented
}

// PauseJob transitions a job into JobStatePaused.
//
// Not yet implemented in this scaffold.
func (r *Registry) PauseJob(ctx context.Context, id string) error {
	return dberrors.ErrNotImplemented
}

// ResumeJob transitions a job into JobStateRunning.
//
// Not yet implemented in this scaffold.
func (r *Registry) ResumeJob(ctx context.Context, id string) error {
	return dberrors.ErrNotImplemented
}

// StopJob transitions a job into JobStateStopped. This transition is
// terminal.
//
// Not yet implemented in this scaffold.
func (r *Registry) StopJob(ctx context.Context, id string) error {
	return dberrors.ErrNotImplemented
}

// ListStreams returns every registered Stream, sorted by ID.
//
// Not yet implemented in this scaffold.
func (r *Registry) ListStreams(ctx context.Context) ([]Stream, error) {
	return nil, dberrors.ErrNotImplemented
}

// ListJobs returns every registered Job, sorted by ID.
//
// Not yet implemented in this scaffold.
func (r *Registry) ListJobs(ctx context.Context) ([]Job, error) {
	return nil, dberrors.ErrNotImplemented
}

// Snapshot returns a Snapshot of every Stream and Job, enriched with
// checkpoint and lag data from cdcStore and apply statistics from loop.
// Either may be nil, in which case the corresponding enrichment is
// skipped.
//
// Not yet implemented in this scaffold.
func (r *Registry) Snapshot(ctx context.Context, cdcStore *cdc.Store, loop *xcluster.Loop) (Snapshot, error) {
	return Snapshot{}, dberrors.ErrNotImplemented
}
