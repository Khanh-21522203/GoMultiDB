package tablet_test

import (
	"context"
	"testing"

	"GoMultiDB/internal/partition"
	"GoMultiDB/internal/tablet"
)

func TestCreateTabletIdempotent(t *testing.T) {
	m := tablet.NewManager()
	meta := tablet.Meta{
		TabletID:  "tablet-a",
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("m")},
	}
	if err := m.CreateTablet(context.Background(), meta, "req-1"); err != nil {
		t.Fatalf("create tablet: %v", err)
	}
	if err := m.CreateTablet(context.Background(), meta, "req-1"); err != nil {
		t.Fatalf("idempotent create tablet: %v", err)
	}

	peer, err := m.OpenTablet(context.Background(), "tablet-a")
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	if peer.State != tablet.Running {
		t.Fatalf("expected running state, got %v", peer.State)
	}
}

func TestSplitTabletUpdatesMapAndChildren(t *testing.T) {
	parts, err := partition.CreateInitialPartitions(partition.PartitionSchema{}, [][]byte{[]byte("m")})
	if err != nil {
		t.Fatalf("create initial partitions: %v", err)
	}
	pmap, err := partition.NewMap(parts)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}

	m := tablet.NewManager()
	if err := m.CreateTablet(context.Background(), tablet.Meta{
		TabletID:  "tablet-1",
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte{}, EndKey: []byte("m")},
	}, "req-create"); err != nil {
		t.Fatalf("create tablet: %v", err)
	}

	left, right, err := m.SplitTablet(context.Background(), "tablet-1", []byte("g"), "req-split", pmap)
	if err != nil {
		t.Fatalf("split tablet: %v", err)
	}
	if left == "" || right == "" {
		t.Fatalf("expected child tablet ids")
	}

	got, err := pmap.FindTablet([]byte("b"))
	if err != nil {
		t.Fatalf("route b: %v", err)
	}
	if got != left {
		t.Fatalf("route b=%s want=%s", got, left)
	}
	got, err = pmap.FindTablet([]byte("h"))
	if err != nil {
		t.Fatalf("route h: %v", err)
	}
	if got != right {
		t.Fatalf("route h=%s want=%s", got, right)
	}

	all := m.ListTablets(context.Background())
	if len(all) != 3 {
		t.Fatalf("expected parent replaced by two children in map with original sibling, got %d", len(all))
	}
}

func TestSplitTabletRollsBackOnPartitionMapFailure(t *testing.T) {
	parts, err := partition.CreateInitialPartitions(partition.PartitionSchema{}, [][]byte{[]byte("m")})
	if err != nil {
		t.Fatalf("create initial partitions: %v", err)
	}
	pmap, err := partition.NewMap(parts)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}

	m := tablet.NewManager()
	if err := m.CreateTablet(context.Background(), tablet.Meta{
		TabletID:  "tablet-1",
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte{}, EndKey: []byte("m")},
	}, "req-create"); err != nil {
		t.Fatalf("create tablet: %v", err)
	}

	if err := pmap.RegisterTabletSplit("tablet-1",
		partition.TabletPartition{TabletID: "tablet-1-L", Bound: partition.PartitionBound{StartKey: []byte{}, EndKey: []byte("g")}},
		partition.TabletPartition{TabletID: "tablet-1-R", Bound: partition.PartitionBound{StartKey: []byte("g"), EndKey: []byte("m")}},
	); err != nil {
		t.Fatalf("pre-seed split: %v", err)
	}

	if _, _, err := m.SplitTablet(context.Background(), "tablet-1", []byte("g"), "req-split", pmap); err == nil {
		t.Fatalf("expected split failure due to partition map conflict")
	}

	parent, err := m.OpenTablet(context.Background(), "tablet-1")
	if err != nil {
		t.Fatalf("parent tablet should still exist after rollback: %v", err)
	}
	if parent.State != tablet.Running {
		t.Fatalf("parent should be running after rollback, got %v", parent.State)
	}
	if _, err := m.OpenTablet(context.Background(), "tablet-1-L"); err == nil {
		t.Fatalf("left child should not persist after rollback")
	}
	if _, err := m.OpenTablet(context.Background(), "tablet-1-R"); err == nil {
		t.Fatalf("right child should not persist after rollback")
	}
}

func TestRemoteBootstrapFromTombstoned(t *testing.T) {
	m := tablet.NewManager()
	meta := tablet.Meta{
		TabletID:  "tablet-rb",
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
	}
	if err := m.CreateTablet(context.Background(), meta, "req-1"); err != nil {
		t.Fatalf("create tablet: %v", err)
	}
	if err := m.DeleteTablet(context.Background(), "tablet-rb", true, "req-del"); err != nil {
		t.Fatalf("tombstone tablet: %v", err)
	}
	if err := m.RemoteBootstrapTablet(context.Background(), "tablet-rb", "peer-1"); err != nil {
		t.Fatalf("remote bootstrap tablet: %v", err)
	}

	p, err := m.OpenTablet(context.Background(), "tablet-rb")
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	if p.State != tablet.Running {
		t.Fatalf("expected running after remote bootstrap, got %v", p.State)
	}
}

func TestRemoteBootstrapRequiresTombstonedOrFailed(t *testing.T) {
	m := tablet.NewManager()
	meta := tablet.Meta{
		TabletID:  "tablet-rb-conflict",
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
	}
	if err := m.CreateTablet(context.Background(), meta, "req-1"); err != nil {
		t.Fatalf("create tablet: %v", err)
	}

	if err := m.RemoteBootstrapTablet(context.Background(), "tablet-rb-conflict", "peer-1"); err == nil {
		t.Fatalf("expected conflict when remote bootstrap is requested for running tablet")
	}
}

func TestHardDeleteAfterTombstoneRemovesTablet(t *testing.T) {
	m := tablet.NewManager()
	meta := tablet.Meta{
		TabletID:  "tablet-del",
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
	}
	if err := m.CreateTablet(context.Background(), meta, "req-create"); err != nil {
		t.Fatalf("create tablet: %v", err)
	}
	if err := m.DeleteTablet(context.Background(), "tablet-del", true, "req-tombstone"); err != nil {
		t.Fatalf("tombstone tablet: %v", err)
	}
	if err := m.DeleteTablet(context.Background(), "tablet-del", false, "req-delete"); err != nil {
		t.Fatalf("hard delete tablet: %v", err)
	}
	if _, err := m.OpenTablet(context.Background(), "tablet-del"); err == nil {
		t.Fatalf("expected tablet to be removed after hard delete")
	}
}

func TestSplitTabletRejectsWhenParentNotRunning(t *testing.T) {
	parts, err := partition.CreateInitialPartitions(partition.PartitionSchema{}, [][]byte{[]byte("m")})
	if err != nil {
		t.Fatalf("create initial partitions: %v", err)
	}
	pmap, err := partition.NewMap(parts)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}

	m := tablet.NewManager()
	if err := m.CreateTablet(context.Background(), tablet.Meta{
		TabletID:  "tablet-state",
		TableID:   "table-1",
		Partition: partition.PartitionBound{StartKey: []byte{}, EndKey: []byte("m")},
	}, "req-create"); err != nil {
		t.Fatalf("create tablet: %v", err)
	}
	if err := m.DeleteTablet(context.Background(), "tablet-state", true, "req-tombstone"); err != nil {
		t.Fatalf("tombstone tablet: %v", err)
	}

	if _, _, err := m.SplitTablet(context.Background(), "tablet-state", []byte("g"), "req-split", pmap); err == nil {
		t.Fatalf("expected split to fail when parent is not running")
	}
}
