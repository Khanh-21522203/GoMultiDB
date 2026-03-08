package tablet_test

import (
	"context"
	"testing"

	"GoMultiDB/internal/partition"
	"GoMultiDB/internal/tablet"
)

// newFSManager creates a FS-backed Manager in a temp directory.
func newFSManager(t *testing.T) *tablet.Manager {
	t.Helper()
	dir := t.TempDir()
	m, err := tablet.NewManagerWithFS(dir)
	if err != nil {
		t.Fatalf("NewManagerWithFS: %v", err)
	}
	return m
}

// reloadFSManager reopens a Manager from the same directory (simulates restart).
func reloadFSManager(t *testing.T, dir string) *tablet.Manager {
	t.Helper()
	m, err := tablet.NewManagerWithFS(dir)
	if err != nil {
		t.Fatalf("NewManagerWithFS reload: %v", err)
	}
	return m
}

// dirFrom creates a manager, returns (manager, metaDir) so tests can reload.
func dirFrom(t *testing.T) (*tablet.Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m, err := tablet.NewManagerWithFS(dir)
	if err != nil {
		t.Fatalf("NewManagerWithFS: %v", err)
	}
	return m, dir
}

func sampleMeta(tabletID string) tablet.Meta {
	return tablet.Meta{
		TabletID:  tabletID,
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
	}
}

// ── create and reload ─────────────────────────────────────────────────────────

func TestFSCreateAndReload(t *testing.T) {
	m, dir := dirFrom(t)
	ctx := context.Background()

	if err := m.CreateTablet(ctx, sampleMeta("t1"), "req-1"); err != nil {
		t.Fatalf("create: %v", err)
	}

	m2 := reloadFSManager(t, dir)
	p, err := m2.OpenTablet(ctx, "t1")
	if err != nil {
		t.Fatalf("open after reload: %v", err)
	}
	if p.State != tablet.Running {
		t.Fatalf("want Running, got %v", p.State)
	}
	if p.Meta.StateVersion != 1 {
		t.Fatalf("want StateVersion=1, got %d", p.Meta.StateVersion)
	}
}

func TestFSCreateIdempotentAcrossReload(t *testing.T) {
	m, dir := dirFrom(t)
	ctx := context.Background()

	meta := sampleMeta("t-idem")
	if err := m.CreateTablet(ctx, meta, "req-1"); err != nil {
		t.Fatalf("create: %v", err)
	}

	m2 := reloadFSManager(t, dir)
	// Same meta, same tablet ID — idempotent.
	if err := m2.CreateTablet(ctx, meta, "req-1"); err != nil {
		t.Fatalf("idempotent create after reload: %v", err)
	}
}

// ── tombstone and reload ──────────────────────────────────────────────────────

func TestFSTombstoneAndReload(t *testing.T) {
	m, dir := dirFrom(t)
	ctx := context.Background()

	if err := m.CreateTablet(ctx, sampleMeta("t-tomb"), "req-1"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.DeleteTablet(ctx, "t-tomb", true, "req-del"); err != nil {
		t.Fatalf("tombstone: %v", err)
	}

	m2 := reloadFSManager(t, dir)
	p, err := m2.OpenTablet(ctx, "t-tomb")
	if err != nil {
		t.Fatalf("open after reload: %v", err)
	}
	if p.State != tablet.Tombstoned {
		t.Fatalf("want Tombstoned, got %v", p.State)
	}
}

// ── hard delete and reload ────────────────────────────────────────────────────

func TestFSHardDeleteAndReload(t *testing.T) {
	m, dir := dirFrom(t)
	ctx := context.Background()

	if err := m.CreateTablet(ctx, sampleMeta("t-del"), "req-1"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.DeleteTablet(ctx, "t-del", false, "req-del"); err != nil {
		t.Fatalf("hard delete: %v", err)
	}

	m2 := reloadFSManager(t, dir)
	if _, err := m2.OpenTablet(ctx, "t-del"); err == nil {
		t.Fatalf("tablet should be absent after hard delete and reload")
	}
}

// ── split and reload ──────────────────────────────────────────────────────────

func TestFSSplitAndReload(t *testing.T) {
	m, dir := dirFrom(t)
	ctx := context.Background()

	// Build a partition map so split can register children.
	parts, err := partition.CreateInitialPartitions(partition.PartitionSchema{}, [][]byte{[]byte("z")})
	if err != nil {
		t.Fatalf("create partitions: %v", err)
	}
	pmap, err := partition.NewMap(parts)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}
	// Register our tablet into the map under the first partition bound.
	_ = pmap // we'll use a fresh pmap that includes our tablet

	// Create parent with the exact partition bounds the map uses.
	parentMeta := tablet.Meta{
		TabletID:  "parent",
		TableID:   "table-1",
		Partition: parts[0].Bound, // [empty, z)
	}
	pmap2, err := partition.NewMap([]partition.TabletPartition{
		{TabletID: "parent", Bound: parentMeta.Partition, State: "RUNNING"},
	})
	if err != nil {
		t.Fatalf("pmap2: %v", err)
	}

	if err := m.CreateTablet(ctx, parentMeta, "req-create"); err != nil {
		t.Fatalf("create parent: %v", err)
	}

	leftID, rightID, err := m.SplitTablet(ctx, "parent", []byte("m"), "req-split", pmap2)
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	// Reload and verify.
	m2 := reloadFSManager(t, dir)

	// Parent must be Tombstoned.
	pp, err := m2.OpenTablet(ctx, "parent")
	if err != nil {
		t.Fatalf("open parent after reload: %v", err)
	}
	if pp.State != tablet.Tombstoned {
		t.Fatalf("parent want Tombstoned, got %v", pp.State)
	}

	// Children must be Running.
	for _, id := range []string{leftID, rightID} {
		cp, err := m2.OpenTablet(ctx, id)
		if err != nil {
			t.Fatalf("open child %s after reload: %v", id, err)
		}
		if cp.State != tablet.Running {
			t.Fatalf("child %s want Running, got %v", id, cp.State)
		}
		if cp.Meta.SplitParentID != "parent" {
			t.Fatalf("child %s SplitParentID want parent, got %q", id, cp.Meta.SplitParentID)
		}
	}
}

// ── recovery from incomplete states ──────────────────────────────────────────

func TestFSRecoveryFromBootstrapping(t *testing.T) {
	// Simulate a crash where a meta was written with State=Bootstrapping.
	dir := t.TempDir()
	store, err := tablet.NewFileMetaStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// Write a Bootstrapping marker directly (simulates crash mid-bootstrap).
	stuckMeta := tablet.Meta{
		TabletID:     "t-stuck",
		TableID:      "table-1",
		Partition:    partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
		State:        tablet.Bootstrapping,
		StateVersion: 2,
	}
	if err := store.WriteMeta(stuckMeta); err != nil {
		t.Fatalf("write stuck meta: %v", err)
	}

	m := reloadFSManager(t, dir)
	p, err := m.OpenTablet(context.Background(), "t-stuck")
	if err != nil {
		t.Fatalf("open after recovery: %v", err)
	}
	if p.State != tablet.Failed {
		t.Fatalf("want Failed after recovery from Bootstrapping, got %v", p.State)
	}
	if p.Meta.StateVersion <= stuckMeta.StateVersion {
		t.Fatalf("StateVersion should have incremented on recovery, got %d", p.Meta.StateVersion)
	}
}

func TestFSRecoveryFromSplitting(t *testing.T) {
	dir := t.TempDir()
	store, err := tablet.NewFileMetaStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	// Simulates a crash after writing Splitting but before children.
	stuckMeta := tablet.Meta{
		TabletID:     "t-split",
		TableID:      "table-1",
		Partition:    partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
		State:        tablet.Splitting,
		StateVersion: 3,
	}
	if err := store.WriteMeta(stuckMeta); err != nil {
		t.Fatalf("write stuck meta: %v", err)
	}

	m := reloadFSManager(t, dir)
	p, err := m.OpenTablet(context.Background(), "t-split")
	if err != nil {
		t.Fatalf("open after recovery: %v", err)
	}
	if p.State != tablet.Failed {
		t.Fatalf("want Failed after recovery from Splitting, got %v", p.State)
	}
}

func TestFSRecoveryFromDeleting(t *testing.T) {
	// Simulate crash after writing Deleting but before file was removed.
	dir := t.TempDir()
	store, err := tablet.NewFileMetaStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	deletingMeta := tablet.Meta{
		TabletID:     "t-deleting",
		TableID:      "table-1",
		Partition:    partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
		State:        tablet.Deleting,
		StateVersion: 5,
	}
	if err := store.WriteMeta(deletingMeta); err != nil {
		t.Fatalf("write deleting meta: %v", err)
	}

	// Recovery should complete the deletion.
	m := reloadFSManager(t, dir)
	if _, err := m.OpenTablet(context.Background(), "t-deleting"); err == nil {
		t.Fatalf("tablet in Deleting state should be absent after recovery")
	}

	// Reload again — no stale file should remain.
	m2 := reloadFSManager(t, dir)
	tablets := m2.ListTablets(context.Background())
	for _, p := range tablets {
		if p.Meta.TabletID == "t-deleting" {
			t.Fatalf("stale Deleting tablet survived second reload")
		}
	}
}

// ── multiple tablets survive reload ──────────────────────────────────────────

func TestFSMultipleTabletsReload(t *testing.T) {
	m, dir := dirFrom(t)
	ctx := context.Background()

	ids := []string{"t-a", "t-b", "t-c"}
	for _, id := range ids {
		if err := m.CreateTablet(ctx, tablet.Meta{
			TabletID:  id,
			TableID:   "table-1",
			Partition: partition.PartitionBound{StartKey: []byte(id), EndKey: []byte(id + "z")},
		}, "req-"+id); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	// Tombstone one, hard-delete another.
	if err := m.DeleteTablet(ctx, "t-b", true, "req-tomb-b"); err != nil {
		t.Fatalf("tombstone t-b: %v", err)
	}
	if err := m.DeleteTablet(ctx, "t-c", false, "req-del-c"); err != nil {
		t.Fatalf("delete t-c: %v", err)
	}

	m2 := reloadFSManager(t, dir)
	tablets := m2.ListTablets(ctx)
	byID := make(map[string]tablet.Peer)
	for _, p := range tablets {
		byID[p.Meta.TabletID] = p
	}

	if pa, ok := byID["t-a"]; !ok || pa.State != tablet.Running {
		t.Fatalf("t-a should be Running; ok=%v state=%v", ok, byID["t-a"].State)
	}
	if pb, ok := byID["t-b"]; !ok || pb.State != tablet.Tombstoned {
		t.Fatalf("t-b should be Tombstoned; ok=%v state=%v", ok, byID["t-b"].State)
	}
	if _, ok := byID["t-c"]; ok {
		t.Fatalf("t-c should be absent after hard delete")
	}
}

// ── remote bootstrap persists across reload ───────────────────────────────────

func TestFSRemoteBootstrapAndReload(t *testing.T) {
	m, dir := dirFrom(t)
	ctx := context.Background()

	if err := m.CreateTablet(ctx, sampleMeta("t-rb"), "req-1"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := m.DeleteTablet(ctx, "t-rb", true, "req-tomb"); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	if err := m.RemoteBootstrapTablet(ctx, "t-rb", "peer-1"); err != nil {
		t.Fatalf("remote bootstrap: %v", err)
	}

	m2 := reloadFSManager(t, dir)
	p, err := m2.OpenTablet(ctx, "t-rb")
	if err != nil {
		t.Fatalf("open after reload: %v", err)
	}
	if p.State != tablet.Running {
		t.Fatalf("want Running after remote bootstrap + reload, got %v", p.State)
	}
}
