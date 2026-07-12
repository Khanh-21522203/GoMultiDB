package cdc

import (
	"context"
	"sync"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// SplitRemapper handles CDC checkpoint remapping when parent tablets
// split. When a parent tablet splits into two children, the existing
// checkpoint for (streamID, parentTabletID) must be copied to both
// children so that polling can continue without losing or duplicating
// events.
//
// Scaffold stub: all methods are unimplemented.
type SplitRemapper struct {
	mu              sync.Mutex
	checkpointStore CheckpointStore
	splitRegistry   map[string]SplitInfo
}

// SplitInfo records the children of a split parent tablet.
type SplitInfo struct {
	ParentTabletID string
	LeftChildID    string
	RightChildID   string
	// SplitSeq is the sequence number at which the split occurred.
	SplitSeq uint64
}

// NewSplitRemapper returns a SplitRemapper backed by store.
func NewSplitRemapper(store CheckpointStore) *SplitRemapper {
	return &SplitRemapper{}
}

// RegisterSplit records a tablet split event. The parent checkpoint is
// atomically copied to both children starting from the parent's last
// sequence.
//
// Not yet implemented in this scaffold.
func (sr *SplitRemapper) RegisterSplit(ctx context.Context, streamID string, info SplitInfo) error {
	return dberrors.ErrNotImplemented
}

// GetChildren returns the child tablet IDs for a previously split parent.
// Returns ("", "", false) if the parent is not known to have split.
//
// Not yet implemented in this scaffold.
func (sr *SplitRemapper) GetChildren(parentTabletID string) (left, right string, ok bool) {
	panic("not implemented")
}

// IsSplit reports whether parentTabletID is a known parent that has
// split.
//
// Not yet implemented in this scaffold.
func (sr *SplitRemapper) IsSplit(parentTabletID string) bool {
	panic("not implemented")
}

// ForgetParent removes the split record for parentTabletID. Useful when
// the parent tablet is fully deleted and its children have taken over.
//
// Not yet implemented in this scaffold.
func (sr *SplitRemapper) ForgetParent(parentTabletID string) {
	panic("not implemented")
}

// RemapPoller derives PollerConfigs for the left and right children of a
// split parent from parentPollerCfg, ready to be started in place of the
// parent's poller.
//
// Not yet implemented in this scaffold.
func (sr *SplitRemapper) RemapPoller(ctx context.Context, parentPollerCfg PollerConfig) (leftCfg, rightCfg PollerConfig, err error) {
	return PollerConfig{}, PollerConfig{}, dberrors.ErrNotImplemented
}

// GenerateUniqueTabletID generates a unique tablet ID for testing
// purposes. In production, tablet IDs come from the master.
//
// Not yet implemented in this scaffold.
func GenerateUniqueTabletID() string {
	panic("not implemented")
}
