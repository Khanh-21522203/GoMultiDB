package server

import (
	"context"
	"testing"
	"time"

	"GoMultiDB/internal/master/snapshot"
	rpcpkg "GoMultiDB/internal/rpc"
	"GoMultiDB/internal/storage/rocks"
)

func TestSnapshotCoordinatorIntegration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "snapshot-integration-test"
	cfg.EnableSnapshotCoord = true
	cfg.MaxConcurrentSnaps = 2

	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{BindAddress: "127.0.0.1:0", StrictContractCheck: true})
	if err != nil {
		t.Fatalf("new rpc server: %v", err)
	}

	rocksStore := rocks.NewMemoryStore()
	r, err := NewRuntime(cfg, rpcServer, rocksStore)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	coord := r.GetSnapshotCoordinator()
	if coord == nil {
		t.Fatalf("expected non-nil snapshot coordinator when enabled")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test that we can create a snapshot through the coordinator.
	snapID := "test-snapshot-1"
	tabletIDs := []string{"tablet-1", "tablet-2", "tablet-3"}
	createHT := uint64(12345)

	info, err := coord.CreateSnapshot(ctx, snapID, tabletIDs, createHT)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if info == nil {
		t.Fatalf("expected non-nil snapshot info")
	}
	if info.SnapshotID != snapID {
		t.Fatalf("expected snapshot id %s, got %s", snapID, info.SnapshotID)
	}
	if info.State != snapshot.StateComplete {
		t.Fatalf("expected state COMPLETE, got %s", info.State)
	}
	if info.CreateHT != createHT {
		t.Fatalf("expected create ht %d, got %d", createHT, info.CreateHT)
	}
	if len(info.TabletIDs) != len(tabletIDs) {
		t.Fatalf("expected %d tablets, got %d", len(tabletIDs), len(info.TabletIDs))
	}

	// Test GetSnapshot.
	got, err := coord.GetSnapshot(snapID)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if got.SnapshotID != snapID {
		t.Fatalf("expected snapshot id %s, got %s", snapID, got.SnapshotID)
	}

	// Test ListSnapshots.
	list := coord.ListSnapshots()
	if len(list) != 1 {
		t.Fatalf("expected 1 snapshot in list, got %d", len(list))
	}
	if list[0].SnapshotID != snapID {
		t.Fatalf("expected snapshot id %s in list, got %s", snapID, list[0].SnapshotID)
	}

	// Test DeleteSnapshot.
	if err := coord.DeleteSnapshot(ctx, snapID); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}

	// Verify snapshot is deleted.
	_, err = coord.GetSnapshot(snapID)
	if err == nil {
		t.Fatalf("expected error getting deleted snapshot")
	}
}

func TestSnapshotCoordinatorDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "snapshot-disabled-test"
	cfg.EnableSnapshotCoord = false

	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{BindAddress: "127.0.0.1:0", StrictContractCheck: true})
	if err != nil {
		t.Fatalf("new rpc server: %v", err)
	}

	rocksStore := rocks.NewMemoryStore()
	r, err := NewRuntime(cfg, rpcServer, rocksStore)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	coord := r.GetSnapshotCoordinator()
	if coord != nil {
		t.Fatalf("expected nil snapshot coordinator when disabled")
	}
}

func TestSnapshotCoordinatorRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "snapshot-recovery-test"
	cfg.EnableSnapshotCoord = true

	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{BindAddress: "127.0.0.1:0", StrictContractCheck: true})
	if err != nil {
		t.Fatalf("new rpc server: %v", err)
	}

	rocksStore := rocks.NewMemoryStore()
	snapStore := snapshot.NewRocksSnapshotStore(rocksStore)

	// Pre-populate the store with an in-progress snapshot.
	ctx := context.Background()
	snapID := "recovery-snapshot-1"
	snapInfo := snapshot.SnapshotInfo{
		SnapshotID:   snapID,
		TabletIDs:    []string{"tablet-a", "tablet-b"},
		CreateHT:     9999,
		State:        snapshot.StateCreating,
		CreatedAt:    time.Now().UTC(),
	}
	if err := snapStore.SaveSnapshot(ctx, snapInfo); err != nil {
		t.Fatalf("pre-populate snapshot: %v", err)
	}

	// Create runtime - it should recover the in-progress snapshot.
	r, err := NewRuntime(cfg, rpcServer, rocksStore)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	coord := r.GetSnapshotCoordinator()
	if coord == nil {
		t.Fatalf("expected non-nil snapshot coordinator")
	}

	// Verify the snapshot was recovered.
	got, err := coord.GetSnapshot(snapID)
	if err != nil {
		t.Fatalf("get recovered snapshot: %v", err)
	}
	// The noop RPC should have "completed" the snapshot during recovery.
	if got.State != snapshot.StateComplete {
		t.Fatalf("expected recovered snapshot to be COMPLETE, got %s", got.State)
	}
	if got.SnapshotID != snapID {
		t.Fatalf("expected snapshot id %s, got %s", snapID, got.SnapshotID)
	}
}
