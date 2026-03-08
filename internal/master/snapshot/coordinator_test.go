package snapshot_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/master/snapshot"
)

// ── mock RPC ──────────────────────────────────────────────────────────────────

type mockRPC struct {
	createErr  error
	deleteErr  error
	restoreErr error
	// Track calls.
	createCalls  atomic.Int32
	deleteCalls  atomic.Int32
	restoreCalls atomic.Int32
}

func (m *mockRPC) CreateTabletSnapshot(_ context.Context, _, _ string) error {
	m.createCalls.Add(1)
	return m.createErr
}

func (m *mockRPC) DeleteTabletSnapshot(_ context.Context, _, _ string) error {
	m.deleteCalls.Add(1)
	return m.deleteErr
}

func (m *mockRPC) RestoreTabletSnapshot(_ context.Context, _, _ string) error {
	m.restoreCalls.Add(1)
	return m.restoreErr
}

// ── helpers ───────────────────────────────────────────────────────────────────

func errCode(t *testing.T, err error) dberrors.ErrorCode {
	t.Helper()
	var dbe dberrors.DBError
	if !errors.As(err, &dbe) {
		t.Fatalf("expected DBError, got %T: %v", err, err)
	}
	return dbe.Code
}

func newCoordinator(rpc snapshot.TabletSnapshotRPC) *snapshot.Coordinator {
	return snapshot.NewCoordinator(rpc, snapshot.Config{MaxConcurrentSnapshots: 2})
}

// ── CreateSnapshot ────────────────────────────────────────────────────────────

func TestCreateSnapshotHappyPath(t *testing.T) {
	rpc := &mockRPC{}
	c := newCoordinator(rpc)

	info, err := c.CreateSnapshot(context.Background(), "snap-1", []string{"t1", "t2", "t3"}, 100)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if info.State != snapshot.StateComplete {
		t.Fatalf("expected COMPLETE, got %v", info.State)
	}
	if int(rpc.createCalls.Load()) != 3 {
		t.Fatalf("expected 3 create calls, got %d", rpc.createCalls.Load())
	}
}

func TestCreateSnapshotPartialFailure(t *testing.T) {
	rpc := &mockRPC{createErr: errors.New("disk full")}
	c := newCoordinator(rpc)

	info, err := c.CreateSnapshot(context.Background(), "snap-fail", []string{"t1"}, 1)
	if err == nil {
		t.Fatalf("expected error on RPC failure")
	}
	if info.State != snapshot.StateFailed {
		t.Fatalf("expected FAILED state, got %v", info.State)
	}
}

func TestCreateSnapshotDuplicate(t *testing.T) {
	rpc := &mockRPC{}
	c := newCoordinator(rpc)

	_, err := c.CreateSnapshot(context.Background(), "snap-dup", []string{"t1"}, 1)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err = c.CreateSnapshot(context.Background(), "snap-dup", []string{"t1"}, 2)
	if err == nil {
		t.Fatalf("expected conflict error on duplicate snapshot ID")
	}
	if got := errCode(t, err); got != dberrors.ErrConflict {
		t.Fatalf("expected ErrConflict, got %s", got)
	}
}

func TestCreateSnapshotRequiresTablets(t *testing.T) {
	rpc := &mockRPC{}
	c := newCoordinator(rpc)
	_, err := c.CreateSnapshot(context.Background(), "snap-empty", nil, 1)
	if err == nil {
		t.Fatalf("expected error with no tablet IDs")
	}
}

// ── GetSnapshot ───────────────────────────────────────────────────────────────

func TestGetSnapshot(t *testing.T) {
	rpc := &mockRPC{}
	c := newCoordinator(rpc)
	_, _ = c.CreateSnapshot(context.Background(), "snap-get", []string{"t1"}, 42)

	info, err := c.GetSnapshot("snap-get")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if info.CreateHT != 42 {
		t.Fatalf("CreateHT: want 42, got %d", info.CreateHT)
	}
}

func TestGetSnapshotNotFound(t *testing.T) {
	c := newCoordinator(&mockRPC{})
	_, err := c.GetSnapshot("ghost")
	if err == nil {
		t.Fatalf("expected error for unknown snapshot")
	}
	if got := errCode(t, err); got != dberrors.ErrInvalidArgument {
		t.Fatalf("expected ErrInvalidArgument, got %s", got)
	}
}

// ── DeleteSnapshot ────────────────────────────────────────────────────────────

func TestDeleteSnapshotHappyPath(t *testing.T) {
	rpc := &mockRPC{}
	c := newCoordinator(rpc)
	_, err := c.CreateSnapshot(context.Background(), "snap-del", []string{"t1", "t2"}, 1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := c.DeleteSnapshot(context.Background(), "snap-del"); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if int(rpc.deleteCalls.Load()) != 2 {
		t.Fatalf("expected 2 delete calls, got %d", rpc.deleteCalls.Load())
	}
	// Snapshot removed from registry.
	_, err = c.GetSnapshot("snap-del")
	if err == nil {
		t.Fatalf("expected snapshot to be removed after delete")
	}
}

func TestDeleteSnapshotNotComplete(t *testing.T) {
	rpc := &mockRPC{createErr: errors.New("rpc error")}
	c := newCoordinator(rpc)
	_, _ = c.CreateSnapshot(context.Background(), "snap-fail", []string{"t1"}, 1)

	err := c.DeleteSnapshot(context.Background(), "snap-fail")
	if err == nil {
		t.Fatalf("expected error deleting failed snapshot")
	}
	if got := errCode(t, err); got != dberrors.ErrConflict {
		t.Fatalf("expected ErrConflict, got %s", got)
	}
}

// ── RestoreSnapshot ───────────────────────────────────────────────────────────

func TestRestoreSnapshotHappyPath(t *testing.T) {
	rpc := &mockRPC{}
	c := newCoordinator(rpc)
	_, err := c.CreateSnapshot(context.Background(), "snap-restore", []string{"t1", "t2"}, 1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := c.RestoreSnapshot(context.Background(), "snap-restore"); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	if int(rpc.restoreCalls.Load()) != 2 {
		t.Fatalf("expected 2 restore calls, got %d", rpc.restoreCalls.Load())
	}
	info, _ := c.GetSnapshot("snap-restore")
	if info.State != snapshot.StateComplete {
		t.Fatalf("expected COMPLETE after restore, got %v", info.State)
	}
}

func TestRestoreSnapshotRPCFailure(t *testing.T) {
	rpc := &mockRPC{restoreErr: errors.New("io error")}
	c := newCoordinator(rpc)
	_, _ = c.CreateSnapshot(context.Background(), "snap-rfail", []string{"t1"}, 1)

	err := c.RestoreSnapshot(context.Background(), "snap-rfail")
	if err == nil {
		t.Fatalf("expected error on restore RPC failure")
	}
	info, _ := c.GetSnapshot("snap-rfail")
	if info.State != snapshot.StateFailed {
		t.Fatalf("expected FAILED after restore error, got %v", info.State)
	}
}

// ── ListSnapshots ─────────────────────────────────────────────────────────────

func TestListSnapshots(t *testing.T) {
	rpc := &mockRPC{}
	c := newCoordinator(rpc)
	for i := 0; i < 3; i++ {
		id := "snap-list-" + string(rune('a'+i))
		_, err := c.CreateSnapshot(context.Background(), id, []string{"t1"}, uint64(i))
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	list := c.ListSnapshots()
	if len(list) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(list))
	}
}

// ── concurrent fan-out ────────────────────────────────────────────────────────

func TestCreateSnapshotConcurrentFanOut(t *testing.T) {
	// 10 tablets, cap=2. All should complete.
	rpc := &mockRPC{}
	c := snapshot.NewCoordinator(rpc, snapshot.Config{MaxConcurrentSnapshots: 2})
	tablets := make([]string, 10)
	for i := range tablets {
		tablets[i] = "t" + string(rune('a'+i))
	}
	_, err := c.CreateSnapshot(context.Background(), "snap-fanout", tablets, 1)
	if err != nil {
		t.Fatalf("concurrent fan-out: %v", err)
	}
	if int(rpc.createCalls.Load()) != 10 {
		t.Fatalf("expected 10 create calls, got %d", rpc.createCalls.Load())
	}
}

func TestRecoverResumesCreatingSnapshot(t *testing.T) {
	ctx := context.Background()
	store := &testSnapshotStore{items: map[string]snapshot.SnapshotInfo{
		"snap-recover-create": {
			SnapshotID: "snap-recover-create",
			TabletIDs:  []string{"t1", "t2"},
			State:      snapshot.StateCreating,
		},
	}}
	rpc := &mockRPC{}
	c := snapshot.NewCoordinator(rpc, snapshot.Config{MaxConcurrentSnapshots: 2, Store: store})

	if err := c.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	info, err := c.GetSnapshot("snap-recover-create")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if info.State != snapshot.StateComplete {
		t.Fatalf("expected COMPLETE after recovery, got %v", info.State)
	}
	if int(rpc.createCalls.Load()) != 2 {
		t.Fatalf("expected 2 create calls during recovery, got %d", rpc.createCalls.Load())
	}
}

func TestRecoverResumesDeletingSnapshot(t *testing.T) {
	ctx := context.Background()
	store := &testSnapshotStore{items: map[string]snapshot.SnapshotInfo{
		"snap-recover-delete": {
			SnapshotID: "snap-recover-delete",
			TabletIDs:  []string{"t1"},
			State:      snapshot.StateDeleting,
		},
	}}
	rpc := &mockRPC{}
	c := snapshot.NewCoordinator(rpc, snapshot.Config{MaxConcurrentSnapshots: 2, Store: store})

	if err := c.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err := c.GetSnapshot("snap-recover-delete"); err == nil {
		t.Fatalf("expected deleting snapshot to be removed after recovery")
	}
	if int(rpc.deleteCalls.Load()) != 1 {
		t.Fatalf("expected 1 delete call during recovery, got %d", rpc.deleteCalls.Load())
	}
}

type testSnapshotStore struct {
	items map[string]snapshot.SnapshotInfo
}

func (s *testSnapshotStore) SaveSnapshot(_ context.Context, info snapshot.SnapshotInfo) error {
	if s.items == nil {
		s.items = make(map[string]snapshot.SnapshotInfo)
	}
	s.items[info.SnapshotID] = info
	return nil
}

func (s *testSnapshotStore) DeleteSnapshot(_ context.Context, snapshotID string) error {
	delete(s.items, snapshotID)
	return nil
}

func (s *testSnapshotStore) LoadSnapshots(_ context.Context) ([]snapshot.SnapshotInfo, error) {
	out := make([]snapshot.SnapshotInfo, 0, len(s.items))
	for _, it := range s.items {
		out = append(out, it)
	}
	return out, nil
}
