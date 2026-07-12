package snapshot

import (
	"context"
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// State is the lifecycle state of a snapshot.
type State int

// Snapshot lifecycle states.
const (
	StateCreating State = iota
	StateComplete
	StateFailed
	StateDeleting
	StateRestoring
)

// String renders the State as its uppercase name, or "UNKNOWN" for an
// unrecognized value.
func (s State) String() string {
	switch s {
	case StateCreating:
		return "CREATING"
	case StateComplete:
		return "COMPLETE"
	case StateFailed:
		return "FAILED"
	case StateDeleting:
		return "DELETING"
	case StateRestoring:
		return "RESTORING"
	default:
		return "UNKNOWN"
	}
}

// SnapshotInfo is the coordinator-side descriptor for one snapshot.
type SnapshotInfo struct {
	SnapshotID   string
	NamespaceIDs []string
	TableIDs     []string
	TabletIDs    []string
	CreateHT     uint64
	State        State
	CreatedAt    time.Time
	// Error is set when State == StateFailed.
	Error string
}

// TabletSnapshotTask tracks per-tablet task state for a snapshot.
type TabletSnapshotTask struct {
	SnapshotID string
	TabletID   string
	State      State
	Error      string
}

// TabletSnapshotRPC is the interface the coordinator uses to issue
// per-tablet snapshot operations.
type TabletSnapshotRPC interface {
	// CreateTabletSnapshot creates a snapshot on the tablet.
	CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error
	// DeleteTabletSnapshot deletes a snapshot from the tablet.
	DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error
	// RestoreTabletSnapshot restores a snapshot on the tablet.
	RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error
}

// SnapshotStore persists snapshot descriptors so in-progress snapshots
// can be resumed after a restart.
type SnapshotStore interface {
	// SaveSnapshot persists or updates info.
	SaveSnapshot(ctx context.Context, info SnapshotInfo) error
	// DeleteSnapshot removes the descriptor for snapshotID.
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	// LoadSnapshots returns all persisted snapshot descriptors.
	LoadSnapshots(ctx context.Context) ([]SnapshotInfo, error)
}

// Config controls coordinator behaviour.
type Config struct {
	// MaxConcurrentSnapshots limits in-flight fan-out goroutines per
	// snapshot.
	MaxConcurrentSnapshots int
	// NowFn returns the current time (injectable for tests).
	NowFn func() time.Time
	// Store persists snapshot descriptors for restart recovery.
	Store SnapshotStore
}

// Coordinator manages the lifecycle of distributed snapshots, fanning out
// per-tablet operations via a TabletSnapshotRPC and persisting descriptors
// through a SnapshotStore.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Coordinator struct {
	mu        sync.Mutex
	snapshots map[string]*SnapshotInfo
	rpc       TabletSnapshotRPC
	cfg       Config
	store     SnapshotStore
}

// NewCoordinator creates a Coordinator, applying config defaults and
// recovering any in-progress snapshots from cfg.Store.
func NewCoordinator(rpc TabletSnapshotRPC, cfg Config) *Coordinator {
	return &Coordinator{}
}

// CreateSnapshot creates a new snapshot for the given tablets, fanning out
// CreateTabletSnapshot RPCs and aggregating results. Returns the resulting
// SnapshotInfo.
//
// Not yet implemented in this scaffold.
func (c *Coordinator) CreateSnapshot(ctx context.Context, snapshotID string, tabletIDs []string, createHT uint64) (*SnapshotInfo, error) {
	return nil, errors.ErrNotImplemented
}

// DeleteSnapshot transitions a Complete snapshot to Deleting and fans out
// delete tasks to all involved tablets.
//
// Not yet implemented in this scaffold.
func (c *Coordinator) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return errors.ErrNotImplemented
}

// RestoreSnapshot fans out restore RPCs to all tablets in the snapshot.
// The snapshot must be in the Complete state.
//
// Not yet implemented in this scaffold.
func (c *Coordinator) RestoreSnapshot(ctx context.Context, snapshotID string) error {
	return errors.ErrNotImplemented
}

// GetSnapshot returns a copy of the SnapshotInfo for the given ID.
//
// Not yet implemented in this scaffold.
func (c *Coordinator) GetSnapshot(snapshotID string) (SnapshotInfo, error) {
	return SnapshotInfo{}, errors.ErrNotImplemented
}

// ListSnapshots returns a copy of all snapshot descriptors.
//
// Not yet implemented in this scaffold.
func (c *Coordinator) ListSnapshots() []SnapshotInfo {
	panic("not implemented")
}

// Recover reloads snapshots from the persistent store and re-fans-out any
// snapshot left in the CREATING, RESTORING, or DELETING state.
//
// Not yet implemented in this scaffold.
func (c *Coordinator) Recover(ctx context.Context) error {
	return errors.ErrNotImplemented
}
