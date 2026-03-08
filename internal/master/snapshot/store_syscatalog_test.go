package snapshot_test

import (
	"context"
	"testing"

	"GoMultiDB/internal/master/snapshot"
	"GoMultiDB/internal/storage/rocks"
)

func TestSysCatalogStoreSaveLoadDelete(t *testing.T) {
	ctx := context.Background()
	store := snapshot.NewSysCatalogStore(rocks.NewMemoryStore())

	a := snapshot.SnapshotInfo{SnapshotID: "snap-a", TabletIDs: []string{"t1"}, State: snapshot.StateCreating}
	b := snapshot.SnapshotInfo{SnapshotID: "snap-b", TabletIDs: []string{"t2", "t3"}, State: snapshot.StateComplete}

	if err := store.SaveSnapshot(ctx, a); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := store.SaveSnapshot(ctx, b); err != nil {
		t.Fatalf("save b: %v", err)
	}

	items, err := store.LoadSnapshots(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(items))
	}

	if err := store.DeleteSnapshot(ctx, "snap-a"); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	items, err = store.LoadSnapshots(ctx)
	if err != nil {
		t.Fatalf("load after delete: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 snapshot after delete, got %d", len(items))
	}
	if items[0].SnapshotID != "snap-b" {
		t.Fatalf("expected remaining snap-b, got %s", items[0].SnapshotID)
	}
}

func TestCoordinatorRecoverWithSysCatalogStore(t *testing.T) {
	ctx := context.Background()
	rpc := &mockRPC{}
	db := rocks.NewMemoryStore()
	persist := snapshot.NewSysCatalogStore(db)

	c1 := snapshot.NewCoordinator(rpc, snapshot.Config{MaxConcurrentSnapshots: 2, Store: persist})
	if _, err := c1.CreateSnapshot(ctx, "snap-recover-sys", []string{"t1", "t2"}, 7); err != nil {
		t.Fatalf("create: %v", err)
	}

	c2 := snapshot.NewCoordinator(rpc, snapshot.Config{MaxConcurrentSnapshots: 2, Store: persist})
	got, err := c2.GetSnapshot("snap-recover-sys")
	if err != nil {
		t.Fatalf("get recovered snapshot: %v", err)
	}
	if got.State != snapshot.StateComplete {
		t.Fatalf("expected COMPLETE after recover, got %v", got.State)
	}
}
