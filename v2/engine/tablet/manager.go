package tablet

import (
	"context"
	"sync"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/engine/partition"
)

// State is a tablet's lifecycle state.
type State int

// Tablet lifecycle states.
const (
	NotStarted State = iota
	Bootstrapping
	Running
	Splitting
	Tombstoned
	RemoteBootstrapping
	Deleting
	Deleted
	Failed
)

// String renders s as its upper-snake-case name, e.g. "RUNNING".
//
// Not yet implemented in this scaffold.
func (s State) String() string {
	panic("not implemented")
}

// AnyStateVersion, passed as an expected-state-version precondition,
// disables the state-version check.
const AnyStateVersion uint64 = 0

// AnyOwnerEpoch, passed as an expected-owner-epoch precondition, disables
// the owner-epoch check.
const AnyOwnerEpoch uint64 = 0

// TransferState is a tablet's ownership-transfer state.
type TransferState string

// Tablet ownership-transfer states.
const (
	TransferStateNone     TransferState = "NONE"
	TransferStatePrepared TransferState = "TRANSFER_PREPARED"
)

// Meta is a tablet's durable lifecycle and ownership metadata.
type Meta struct {
	TabletID      string
	TableID       string
	Partition     partition.PartitionBound
	SplitParentID string
	SplitDepth    uint32
	OwnerTSUUID   string
	OwnerEpoch    uint64
	TransferState TransferState
	TransferEpoch uint64
	PendingOwner  string
	State         State
	StateVersion  uint64
}

// Peer is a tablet's in-memory runtime record: its durable Meta plus the
// last observed State and error, if any.
type Peer struct {
	Meta      Meta
	State     State
	LastError string
}

// Manager tracks every tablet hosted by this node and drives their
// lifecycle transitions.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented or panic.
type Manager struct {
	mu    sync.RWMutex
	peers map[string]*Peer
	ops   map[string]bool
	store MetaStore
}

// NewManager returns an in-memory-only Manager (no filesystem durability).
// Use this for tests or single-process ephemeral workloads.
func NewManager() *Manager {
	return &Manager{}
}

// NewManagerWithFS returns a Manager backed by a FileMetaStore rooted at
// metaDir. On startup it scans metaDir for existing meta files and applies
// recovery rules: Running/Tombstoned/Failed tablets are restored as-is;
// in-flight Deleting tablets have their deletion completed;
// NotStarted/Bootstrapping/Splitting/RemoteBootstrapping tablets are
// promoted to Failed; stale Deleted markers are removed.
func NewManagerWithFS(metaDir string) (*Manager, error) {
	return &Manager{}, nil
}

// CreateTablet registers a new tablet under meta.TabletID, persisting it as
// Running. Idempotent when retried with equivalent metadata.
//
// Not yet implemented in this scaffold.
func (m *Manager) CreateTablet(ctx context.Context, meta Meta, reqID string) error {
	return errors.ErrNotImplemented
}

// OpenTablet returns a copy of the Peer for tabletID.
//
// Not yet implemented in this scaffold.
func (m *Manager) OpenTablet(ctx context.Context, tabletID string) (*Peer, error) {
	return nil, errors.ErrNotImplemented
}

// DeleteTablet deletes or tombstones tabletID, without a state-version
// precondition.
//
// Not yet implemented in this scaffold.
func (m *Manager) DeleteTablet(ctx context.Context, tabletID string, tombstone bool, reqID string) error {
	return errors.ErrNotImplemented
}

// DeleteTabletWithExpectedStateVersion deletes or tombstones tabletID,
// failing with ErrConflict if its current StateVersion does not match
// expectedStateVersion (unless expectedStateVersion is AnyStateVersion).
//
// Not yet implemented in this scaffold.
func (m *Manager) DeleteTabletWithExpectedStateVersion(ctx context.Context, tabletID string, tombstone bool, reqID string, expectedStateVersion uint64) error {
	return errors.ErrNotImplemented
}

// SplitTablet splits tabletID at splitKey into two children, registering
// the split in pmap, without a state-version precondition.
//
// Not yet implemented in this scaffold.
func (m *Manager) SplitTablet(ctx context.Context, tabletID string, splitKey []byte, reqID string, pmap *partition.Map) (leftID, rightID string, err error) {
	return "", "", errors.ErrNotImplemented
}

// SplitTabletWithExpectedStateVersion splits tabletID at splitKey into two
// children, registering the split in pmap, failing with ErrConflict if its
// current StateVersion does not match expectedStateVersion.
//
// Not yet implemented in this scaffold.
func (m *Manager) SplitTabletWithExpectedStateVersion(ctx context.Context, tabletID string, splitKey []byte, reqID string, pmap *partition.Map, expectedStateVersion uint64) (leftID, rightID string, err error) {
	return "", "", errors.ErrNotImplemented
}

// RemoteBootstrapTablet restores tabletID to Running by
// remote-bootstrapping its data from sourcePeer, without a state-version
// precondition.
//
// Not yet implemented in this scaffold.
func (m *Manager) RemoteBootstrapTablet(ctx context.Context, tabletID string, sourcePeer string) error {
	return errors.ErrNotImplemented
}

// RemoteBootstrapTabletWithExpectedStateVersion restores tabletID to
// Running by remote-bootstrapping its data from sourcePeer, failing with
// ErrConflict if its current StateVersion does not match
// expectedStateVersion.
//
// Not yet implemented in this scaffold.
func (m *Manager) RemoteBootstrapTabletWithExpectedStateVersion(ctx context.Context, tabletID string, sourcePeer string, expectedStateVersion uint64) error {
	return errors.ErrNotImplemented
}

// TransferPrepare marks tabletID as prepared to transfer ownership from
// fromOwner to toOwner, subject to the expectedOwnerEpoch and
// expectedStateVersion preconditions. Returns the updated Meta.
//
// Not yet implemented in this scaffold.
func (m *Manager) TransferPrepare(ctx context.Context, tabletID, fromOwner, toOwner string, expectedOwnerEpoch, expectedStateVersion uint64) (Meta, error) {
	return Meta{}, errors.ErrNotImplemented
}

// TransferCommit completes a prepared ownership transfer for tabletID,
// subject to the expectedTransferEpoch and expectedStateVersion
// preconditions. Returns the updated Meta.
//
// Not yet implemented in this scaffold.
func (m *Manager) TransferCommit(ctx context.Context, tabletID string, expectedTransferEpoch, expectedStateVersion uint64) (Meta, error) {
	return Meta{}, errors.ErrNotImplemented
}

// ListTablets returns a snapshot copy of every tracked Peer.
//
// Not yet implemented in this scaffold.
func (m *Manager) ListTablets(ctx context.Context) []Peer {
	panic("not implemented")
}
