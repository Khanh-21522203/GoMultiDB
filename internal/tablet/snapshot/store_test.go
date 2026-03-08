// Package snapshot provides tablet-level snapshot storage and operations.
package snapshot

import (
	"context"
	"testing"

	"GoMultiDB/internal/storage/rocks"
)

func TestStore_CreateSnapshot(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())

	// Add some test data to the store
	rs := rocks.NewMemoryStore()
	ctx := context.Background()
	wb := rocks.WriteBatch{
		Ops: []rocks.KV{
			{Key: []byte("key1"), Value: []byte("value1")},
			{Key: []byte("key2"), Value: []byte("value2")},
		},
	}
	if err := rs.ApplyWriteBatch(ctx, rocks.RegularDB, wb); err != nil {
		t.Fatalf("setup: write test data: %v", err)
	}
	store.rocksStore = rs

	data, err := store.CreateSnapshot(ctx, "snap1", "tablet1", 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if data.SnapshotID != "snap1" {
		t.Errorf("expected snapshot id snap1, got %s", data.SnapshotID)
	}
	if data.TabletID != "tablet1" {
		t.Errorf("expected tablet id tablet1, got %s", data.TabletID)
	}
	if data.CreateHT != 100 {
		t.Errorf("expected create ht 100, got %d", data.CreateHT)
	}
	if len(data.RegularData) != 2 {
		t.Errorf("expected 2 regular data entries, got %d", len(data.RegularData))
	}
}

func TestStore_CreateSnapshotDuplicate(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	ctx := context.Background()

	_, err := store.CreateSnapshot(ctx, "snap1", "tablet1", 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	_, err = store.CreateSnapshot(ctx, "snap1", "tablet1", 200)
	if err == nil {
		t.Fatalf("expected error for duplicate snapshot")
	}
}

func TestStore_DeleteSnapshot(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	ctx := context.Background()

	_, err := store.CreateSnapshot(ctx, "snap1", "tablet1", 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	err = store.DeleteSnapshot(ctx, "snap1")
	if err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}

	_, err = store.GetSnapshot(ctx, "snap1")
	if err == nil {
		t.Fatalf("expected error getting deleted snapshot")
	}
}

func TestStore_DeleteSnapshotNotFound(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	ctx := context.Background()

	err := store.DeleteSnapshot(ctx, "nonexistent")
	if err == nil {
		t.Fatalf("expected error for nonexistent snapshot")
	}
}

func TestStore_RestoreSnapshot(t *testing.T) {
	rs := rocks.NewMemoryStore()
	store := NewStore(rs)
	ctx := context.Background()

	// Write some test data
	wb := rocks.WriteBatch{
		Ops: []rocks.KV{
			{Key: []byte("key1"), Value: []byte("value1")},
		},
	}
	if err := rs.ApplyWriteBatch(ctx, rocks.RegularDB, wb); err != nil {
		t.Fatalf("setup: write test data: %v", err)
	}

	// Create snapshot
	_, err := store.CreateSnapshot(ctx, "snap1", "tablet1", 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	// Modify the data
	wb2 := rocks.WriteBatch{
		Ops: []rocks.KV{
			{Key: []byte("key1"), Value: []byte("value2")},
		},
	}
	if err := rs.ApplyWriteBatch(ctx, rocks.RegularDB, wb2); err != nil {
		t.Fatalf("modify data: %v", err)
	}

	// Restore from snapshot
	if err := store.RestoreSnapshot(ctx, "snap1"); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	// Verify data was restored
	val, ok, err := rs.Get(ctx, rocks.RegularDB, []byte("key1"))
	if err != nil {
		t.Fatalf("get restored value: %v", err)
	}
	if !ok {
		t.Fatalf("restored key not found")
	}
	if string(val) != "value1" {
		t.Errorf("expected restored value 'value1', got '%s'", string(val))
	}
}

func TestStore_ListSnapshots(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	ctx := context.Background()

	// Create multiple snapshots
	for i := 1; i <= 3; i++ {
		_, err := store.CreateSnapshot(ctx, "snap"+string(rune('0'+i)), "tablet1", uint64(i*100))
		if err != nil {
			t.Fatalf("create snapshot %d: %v", i, err)
		}
	}

	snapshots := store.ListSnapshots(ctx)
	if len(snapshots) != 3 {
		t.Errorf("expected 3 snapshots, got %d", len(snapshots))
	}
}

func TestStore_Recover(t *testing.T) {
	rs := rocks.NewMemoryStore()
	store := NewStore(rs)
	ctx := context.Background()

	// Create a snapshot
	_, err := store.CreateSnapshot(ctx, "snap1", "tablet1", 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	// Create a new store instance and recover
	store2 := NewStore(rs)
	if err := store2.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Verify snapshot was recovered
	data, err := store2.GetSnapshot(ctx, "snap1")
	if err != nil {
		t.Fatalf("get recovered snapshot: %v", err)
	}
	if data.SnapshotID != "snap1" {
		t.Errorf("expected recovered snapshot id snap1, got %s", data.SnapshotID)
	}
}
