package cdc

import (
	"context"
	"testing"
	"time"
)

// mockCheckpointStore is a simple in-memory CheckpointStore for tests.
type mockCheckpointStore struct {
	checkpoints map[string]Checkpoint // keyed by streamID:tabletID
}

func newMockCheckpointStore() *mockCheckpointStore {
	return &mockCheckpointStore{
		checkpoints: make(map[string]Checkpoint),
	}
}

func (m *mockCheckpointStore) key(streamID, tabletID string) string {
	return streamID + ":" + tabletID
}

func (m *mockCheckpointStore) AdvanceCheckpoint(_ context.Context, cp Checkpoint) error {
	m.checkpoints[m.key(cp.StreamID, cp.TabletID)] = cp
	return nil
}

func (m *mockCheckpointStore) GetCheckpoint(_ context.Context, streamID, tabletID string) (Checkpoint, error) {
	cp, ok := m.checkpoints[m.key(streamID, tabletID)]
	if !ok {
		return Checkpoint{}, nil // return zero checkpoint if not found
	}
	return cp, nil
}

func TestSplitRemapperRegisterSplit(t *testing.T) {
	ctx := context.Background()
	store := newMockCheckpointStore()
	sr := NewSplitRemapper(store)

	// Set up parent checkpoint.
	parentCP := Checkpoint{
		StreamID:  "stream-1",
		TabletID:  "parent-tablet",
		Sequence:       100,
		Timestamp: time.Now().UTC(),
	}
	if err := store.AdvanceCheckpoint(ctx, parentCP); err != nil {
		t.Fatalf("set parent checkpoint: %v", err)
	}

	// Register split.
	info := SplitInfo{
		ParentTabletID: "parent-tablet",
		LeftChildID:    "left-child",
		RightChildID:   "right-child",
		SplitSeq:       100,
	}
	if err := sr.RegisterSplit(ctx, "stream-1", info); err != nil {
		t.Fatalf("RegisterSplit: %v", err)
	}

	// Verify left child checkpoint.
	leftCP, err := store.GetCheckpoint(ctx, "stream-1", "left-child")
	if err != nil {
		t.Fatalf("get left child checkpoint: %v", err)
	}
	if leftCP.Sequence != 100 {
		t.Fatalf("left child seq: want 100, got %d", leftCP.Sequence)
	}

	// Verify right child checkpoint.
	rightCP, err := store.GetCheckpoint(ctx, "stream-1", "right-child")
	if err != nil {
		t.Fatalf("get right child checkpoint: %v", err)
	}
	if rightCP.Sequence != 100 {
		t.Fatalf("right child seq: want 100, got %d", rightCP.Sequence)
	}
}

func TestSplitRemapperGetChildren(t *testing.T) {
	store := newMockCheckpointStore()
	sr := NewSplitRemapper(store)

	// No split registered.
	_, _, ok := sr.GetChildren("unknown-parent")
	if ok {
		t.Fatalf("expected false for unknown parent")
	}

	// Register a split.
	info := SplitInfo{
		ParentTabletID: "parent-1",
		LeftChildID:    "child-1",
		RightChildID:   "child-2",
	}
	sr.splitRegistry["parent-1"] = info

	// Now it should be found.
	left, right, ok := sr.GetChildren("parent-1")
	if !ok {
		t.Fatalf("expected true for known parent")
	}
	if left != "child-1" {
		t.Fatalf("left child: want child-1, got %s", left)
	}
	if right != "child-2" {
		t.Fatalf("right child: want child-2, got %s", right)
	}
}

func TestSplitRemapperIsSplit(t *testing.T) {
	store := newMockCheckpointStore()
	sr := NewSplitRemapper(store)

	if sr.IsSplit("parent-1") {
		t.Fatalf("expected false for unsplit parent")
	}

	sr.splitRegistry["parent-1"] = SplitInfo{
		ParentTabletID: "parent-1",
		LeftChildID:    "child-1",
		RightChildID:   "child-2",
	}

	if !sr.IsSplit("parent-1") {
		t.Fatalf("expected true for split parent")
	}
}

func TestSplitRemapperForgetParent(t *testing.T) {
	store := newMockCheckpointStore()
	sr := NewSplitRemapper(store)

	sr.splitRegistry["parent-1"] = SplitInfo{
		ParentTabletID: "parent-1",
		LeftChildID:    "child-1",
		RightChildID:   "child-2",
	}

	sr.ForgetParent("parent-1")
	if sr.IsSplit("parent-1") {
		t.Fatalf("expected false after forget")
	}
}

func TestSplitRemapperRemapPoller(t *testing.T) {
	ctx := context.Background()
	store := newMockCheckpointStore()
	sr := NewSplitRemapper(store)

	// Register a split.
	info := SplitInfo{
		ParentTabletID: "parent-1",
		LeftChildID:    "child-1",
		RightChildID:   "child-2",
	}
	sr.splitRegistry["parent-1"] = info

	// Remap poller.
	parentCfg := PollerConfig{
		TabletID: "parent-1",
		StreamID: "stream-1",
	}
	leftCfg, rightCfg, err := sr.RemapPoller(ctx, parentCfg)
	if err != nil {
		t.Fatalf("RemapPoller: %v", err)
	}

	if leftCfg.TabletID != "child-1" {
		t.Fatalf("left tablet id: want child-1, got %s", leftCfg.TabletID)
	}
	if rightCfg.TabletID != "child-2" {
		t.Fatalf("right tablet id: want child-2, got %s", rightCfg.TabletID)
	}
	if leftCfg.StreamID != "stream-1" {
		t.Fatalf("left stream id: want stream-1, got %s", leftCfg.StreamID)
	}
}

func TestSplitRemapperRemapPollerUnknown(t *testing.T) {
	ctx := context.Background()
	store := newMockCheckpointStore()
	sr := NewSplitRemapper(store)

	parentCfg := PollerConfig{
		TabletID: "unknown-parent",
		StreamID: "stream-1",
	}
	_, _, err := sr.RemapPoller(ctx, parentCfg)
	if err == nil {
		t.Fatalf("expected error for unknown parent")
	}
}

func TestGenerateUniqueTabletID(t *testing.T) {
	id1 := GenerateUniqueTabletID()
	id2 := GenerateUniqueTabletID()
	if id1 == id2 {
		t.Fatalf("generated IDs should be unique")
	}
	if len(id1) == 0 {
		t.Fatalf("generated ID should not be empty")
	}
}
