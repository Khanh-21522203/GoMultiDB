package tablet

import (
	"context"
	"testing"

	"GoMultiDB/internal/partition"
)

func TestRemoteBootstrapFromFailed(t *testing.T) {
	m := NewManager()
	m.peers["tablet-failed"] = &Peer{
		Meta: Meta{
			TabletID:     "tablet-failed",
			TableID:      "table-1",
			Partition:    partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
			State:        Failed,
			StateVersion: 7,
		},
		State:     Failed,
		LastError: "bootstrap failed",
	}

	if err := m.RemoteBootstrapTablet(context.Background(), "tablet-failed", "peer-1"); err != nil {
		t.Fatalf("remote bootstrap from failed: %v", err)
	}

	p, err := m.OpenTablet(context.Background(), "tablet-failed")
	if err != nil {
		t.Fatalf("open tablet: %v", err)
	}
	if p.State != Running {
		t.Fatalf("expected running after remote bootstrap, got %v", p.State)
	}
	if p.Meta.StateVersion != 9 {
		t.Fatalf("expected state version to increment twice (failed->remote_bootstrapping->running), got %d", p.Meta.StateVersion)
	}
	if p.LastError != "" {
		t.Fatalf("expected last error to be cleared")
	}
}

func TestDeleteTabletWithExpectedStateVersionMismatch(t *testing.T) {
	m := NewManager()
	m.peers["tablet-v"] = &Peer{
		Meta: Meta{
			TabletID:     "tablet-v",
			TableID:      "table-1",
			Partition:    partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
			State:        Running,
			StateVersion: 3,
		},
		State: Running,
	}

	err := m.DeleteTabletWithExpectedStateVersion(context.Background(), "tablet-v", true, "req", 2)
	if err == nil {
		t.Fatalf("expected state version precondition mismatch")
	}
}

func TestSplitTabletWithExpectedStateVersionMismatch(t *testing.T) {
	m := NewManager()
	m.peers["tablet-sv"] = &Peer{
		Meta: Meta{
			TabletID:     "tablet-sv",
			TableID:      "table-1",
			Partition:    partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
			State:        Running,
			StateVersion: 4,
		},
		State: Running,
	}

	if _, _, err := m.SplitTabletWithExpectedStateVersion(context.Background(), "tablet-sv", []byte("m"), "req", nil, 2); err == nil {
		t.Fatalf("expected split state version precondition mismatch")
	}
}

func TestRemoteBootstrapRejectsSplitParentWithActiveChildren(t *testing.T) {
	m := NewManager()
	m.peers["parent"] = &Peer{
		Meta: Meta{
			TabletID:      "parent",
			TableID:       "table-1",
			Partition:     partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("z")},
			State:         Tombstoned,
			StateVersion:  8,
			SplitParentID: "",
		},
		State: Tombstoned,
	}
	m.peers["parent-L"] = &Peer{
		Meta: Meta{
			TabletID:      "parent-L",
			TableID:       "table-1",
			Partition:     partition.PartitionBound{StartKey: []byte("a"), EndKey: []byte("m")},
			State:         Running,
			StateVersion:  1,
			SplitParentID: "parent",
		},
		State: Running,
	}

	err := m.RemoteBootstrapTablet(context.Background(), "parent", "peer-1")
	if err == nil {
		t.Fatalf("expected conflict for split parent with active children")
	}
}
