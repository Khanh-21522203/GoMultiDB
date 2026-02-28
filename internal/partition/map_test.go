package partition_test

import (
	"testing"

	"GoMultiDB/internal/partition"
)

func TestCreateInitialPartitionsAndRouting(t *testing.T) {
	tabs, err := partition.CreateInitialPartitions(partition.PartitionSchema{NumBuckets: 4}, [][]byte{[]byte("m"), []byte("t")})
	if err != nil {
		t.Fatalf("create partitions: %v", err)
	}
	if len(tabs) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(tabs))
	}

	m, err := partition.NewMap(tabs)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}

	cases := []struct {
		key  string
		want string
	}{
		{key: "a", want: "tablet-1"},
		{key: "m", want: "tablet-2"},
		{key: "s", want: "tablet-2"},
		{key: "z", want: "tablet-3"},
	}

	for _, tc := range cases {
		got, err := m.FindTablet([]byte(tc.key))
		if err != nil {
			t.Fatalf("FindTablet(%s): %v", tc.key, err)
		}
		if got != tc.want {
			t.Fatalf("FindTablet(%s)=%s want=%s", tc.key, got, tc.want)
		}
	}
}

func TestRegisterTabletSplit(t *testing.T) {
	tabs, err := partition.CreateInitialPartitions(partition.PartitionSchema{}, [][]byte{[]byte("m")})
	if err != nil {
		t.Fatalf("create partitions: %v", err)
	}
	m, err := partition.NewMap(tabs)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}

	left := partition.TabletPartition{TabletID: "tablet-1a", Bound: partition.PartitionBound{StartKey: []byte{}, EndKey: []byte("g")}}
	right := partition.TabletPartition{TabletID: "tablet-1b", Bound: partition.PartitionBound{StartKey: []byte("g"), EndKey: []byte("m")}}
	if err := m.RegisterTabletSplit("tablet-1", left, right); err != nil {
		t.Fatalf("register split: %v", err)
	}

	got, err := m.FindTablet([]byte("b"))
	if err != nil {
		t.Fatalf("route b: %v", err)
	}
	if got != "tablet-1a" {
		t.Fatalf("route b=%s want tablet-1a", got)
	}
	got, err = m.FindTablet([]byte("k"))
	if err != nil {
		t.Fatalf("route k: %v", err)
	}
	if got != "tablet-1b" {
		t.Fatalf("route k=%s want tablet-1b", got)
	}
	got, err = m.FindTablet([]byte("z"))
	if err != nil {
		t.Fatalf("route z: %v", err)
	}
	if got != "tablet-2" {
		t.Fatalf("route z=%s want tablet-2", got)
	}
	if _, err := m.FindTablet([]byte("m")); err != nil {
		t.Fatalf("route boundary key m: %v", err)
	}
}

func TestRegisterTabletSplitRejectsInvalidChildren(t *testing.T) {
	tabs, err := partition.CreateInitialPartitions(partition.PartitionSchema{}, [][]byte{[]byte("m")})
	if err != nil {
		t.Fatalf("create partitions: %v", err)
	}
	m, err := partition.NewMap(tabs)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}

	left := partition.TabletPartition{TabletID: "tablet-1a", Bound: partition.PartitionBound{StartKey: []byte{}, EndKey: []byte("h")}}
	right := partition.TabletPartition{TabletID: "tablet-1b", Bound: partition.PartitionBound{StartKey: []byte("i"), EndKey: []byte("m")}}
	if err := m.RegisterTabletSplit("tablet-1", left, right); err == nil {
		t.Fatalf("expected split bounds validation error")
	}
}

func TestListOverlapping(t *testing.T) {
	tabs, err := partition.CreateInitialPartitions(partition.PartitionSchema{}, [][]byte{[]byte("m"), []byte("t")})
	if err != nil {
		t.Fatalf("create partitions: %v", err)
	}
	m, err := partition.NewMap(tabs)
	if err != nil {
		t.Fatalf("new map: %v", err)
	}

	over, err := m.ListOverlapping([]byte("h"), []byte("u"))
	if err != nil {
		t.Fatalf("list overlapping: %v", err)
	}
	if len(over) != 3 {
		t.Fatalf("expected 3 overlapping tablets, got %d", len(over))
	}
}
