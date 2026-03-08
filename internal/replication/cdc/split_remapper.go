package cdc

import (
	"context"
	"fmt"
	"sync"

	"GoMultiDB/internal/common/ids"
)

// SplitRemapper handles CDC checkpoint remapping when parent tablets split.
// When a parent tablet splits into two children, the existing checkpoint for
// (streamID, parentTabletID) must be copied to both children so that polling
// can continue without losing or duplicating events.
type SplitRemapper struct {
	mu             sync.Mutex
	checkpointStore CheckpointStore
	splitRegistry   map[string]SplitInfo // parentTabletID -> SplitInfo
}

// SplitInfo records the children of a split parent tablet.
type SplitInfo struct {
	ParentTabletID string
	LeftChildID    string
	RightChildID   string
	SplitSeq       uint64 // Seq at which split occurred
}

// NewSplitRemapper creates a SplitRemapper.
func NewSplitRemapper(store CheckpointStore) *SplitRemapper {
	return &SplitRemapper{
		checkpointStore: store,
		splitRegistry:   make(map[string]SplitInfo),
	}
}

// RegisterSplit records a tablet split event. The parent checkpoint is
// atomically copied to both children starting from the parent's last Seq.
func (sr *SplitRemapper) RegisterSplit(ctx context.Context, streamID string, info SplitInfo) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	// Load parent checkpoint.
	parentCP, err := sr.checkpointStore.GetCheckpoint(ctx, streamID, info.ParentTabletID)
	if err != nil {
		return fmt.Errorf("get parent checkpoint: %w", err)
	}

	// Copy to left child.
	leftCP := Checkpoint{
		StreamID:  streamID,
		TabletID:  info.LeftChildID,
		Sequence:  parentCP.Sequence,
		Timestamp: parentCP.Timestamp,
	}
	if err := sr.checkpointStore.AdvanceCheckpoint(ctx, leftCP); err != nil {
		return fmt.Errorf("advance left child checkpoint: %w", err)
	}

	// Copy to right child.
	rightCP := Checkpoint{
		StreamID:  streamID,
		TabletID:  info.RightChildID,
		Sequence:  parentCP.Sequence,
		Timestamp: parentCP.Timestamp,
	}
	if err := sr.checkpointStore.AdvanceCheckpoint(ctx, rightCP); err != nil {
		return fmt.Errorf("advance right child checkpoint: %w", err)
	}

	// Remember this split.
	sr.splitRegistry[info.ParentTabletID] = info
	return nil
}

// GetChildren returns the child tablet IDs for a previously split parent.
// Returns (nil, false) if the parent is not known to have split.
func (sr *SplitRemapper) GetChildren(parentTabletID string) (left, right string, ok bool) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	info, exists := sr.splitRegistry[parentTabletID]
	if !exists {
		return "", "", false
	}
	return info.LeftChildID, info.RightChildID, true
}

// IsSplit returns true if the given tablet ID is a known parent that has split.
func (sr *SplitRemapper) IsSplit(parentTabletID string) bool {
	_, _, ok := sr.GetChildren(parentTabletID)
	return ok
}

// ForgetParent removes the split record for a parent. Useful when the parent
// tablet is fully deleted and its children have taken over.
func (sr *SplitRemapper) ForgetParent(parentTabletID string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	delete(sr.splitRegistry, parentTabletID)
}

// RemapPoller creates new pollers for the child tablets of a split parent,
// stopping the parent poller. Returns the left and right child PollerConfigs
// ready to be started.
func (sr *SplitRemapper) RemapPoller(
	ctx context.Context,
	parentPollerCfg PollerConfig,
) (leftCfg, rightCfg PollerConfig, err error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	info, ok := sr.splitRegistry[parentPollerCfg.TabletID]
	if !ok {
		return PollerConfig{}, PollerConfig{}, fmt.Errorf("tablet %q not found in split registry", parentPollerCfg.TabletID)
	}

	leftCfg = parentPollerCfg
	leftCfg.TabletID = info.LeftChildID

	rightCfg = parentPollerCfg
	rightCfg.TabletID = info.RightChildID

	return leftCfg, rightCfg, nil
}

// GenerateUniqueTabletID generates a unique tablet ID for testing purposes.
// In production, tablet IDs come from the master.
func GenerateUniqueTabletID() string {
	id, err := ids.NewRequestID()
	if err != nil {
		panic(err)
	}
	return string(id)
}
