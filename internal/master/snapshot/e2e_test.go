// Package snapshot provides the distributed snapshot coordinator.
package snapshot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/storage/rocks"
)

// TestE2E_CreateSnapshotViaRPC tests the end-to-end snapshot flow:
// 1. Master creates snapshot via RPC
// 2. Coordinator fans out to tablets
// 3. Tablet servers handle snapshot operations
// 4. Snapshot is persisted and can be queried
func TestE2E_CreateSnapshotViaRPC(t *testing.T) {
	// Set up mock tablet servers
	var mu sync.Mutex
	capturedSnapshots := make([]string, 0)

	tablet1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		capturedSnapshots = append(capturedSnapshots, "tablet1")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer tablet1.Close()

	tablet2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		capturedSnapshots = append(capturedSnapshots, "tablet2")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer tablet2.Close()

	// Create a tablet registry
	registry := &mockTabletRegistry{
		endpoints: map[string]string{
			"tablet1": tablet1.URL,
			"tablet2": tablet2.URL,
		},
	}

	// Create coordinator with registry
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	rpcClient := NewRegistryClient(registry)
	coord := NewCoordinator(rpcClient, Config{
		Store: snapStore,
		NowFn:  (&fixedClock{}).Now,
	})

	ctx := context.Background()
	snapID := "e2e-snapshot-1"
	tabletIDs := []string{"tablet1", "tablet2"}
	createHT := uint64(12345)

	info, err := coord.CreateSnapshot(ctx, snapID, tabletIDs, createHT)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	if info.SnapshotID != snapID {
		t.Errorf("expected snapshot id %s, got %s", snapID, info.SnapshotID)
	}
	if info.State != StateComplete {
		t.Errorf("expected state COMPLETE, got %s", info.State)
	}

	// Verify both tablets were called
	mu.Lock()
	if len(capturedSnapshots) != 2 {
		t.Errorf("expected 2 tablet calls, got %d", len(capturedSnapshots))
	}
	mu.Unlock()

	// Verify snapshot can be retrieved
	got, err := coord.GetSnapshot(snapID)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if got.SnapshotID != snapID {
		t.Errorf("expected snapshot id %s, got %s", snapID, got.SnapshotID)
	}
}

// TestE2E_RestoreSnapshotViaRPC tests the restore flow.
func TestE2E_RestoreSnapshotViaRPC(t *testing.T) {
	restoreCalled := false
	tablet1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		if req["method"] == "restore_tablet_snapshot" {
			restoreCalled = true
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer tablet1.Close()

	registry := &mockTabletRegistry{
		endpoints: map[string]string{
			"tablet1": tablet1.URL,
		},
	}

	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	rpcClient := NewRegistryClient(registry)
	coord := NewCoordinator(rpcClient, Config{
		Store: snapStore,
		NowFn:  (&fixedClock{}).Now,
	})

	ctx := context.Background()
	snapID := "e2e-restore-1"
	tabletIDs := []string{"tablet1"}

	// First create a snapshot
	_, err := coord.CreateSnapshot(ctx, snapID, tabletIDs, 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	// Then restore it
	if err := coord.RestoreSnapshot(ctx, snapID); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	if !restoreCalled {
		t.Errorf("expected restore to be called on tablet")
	}

	// Verify snapshot is still COMPLETE after restore
	got, err := coord.GetSnapshot(snapID)
	if err != nil {
		t.Fatalf("get snapshot after restore: %v", err)
	}
	if got.State != StateComplete {
		t.Errorf("expected state COMPLETE after restore, got %s", got.State)
	}
}

// TestE2E_DeleteSnapshotViaRPC tests the delete flow.
func TestE2E_DeleteSnapshotViaRPC(t *testing.T) {
	deleteCalled := false
	tablet1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		if req["method"] == "delete_tablet_snapshot" {
			deleteCalled = true
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer tablet1.Close()

	registry := &mockTabletRegistry{
		endpoints: map[string]string{
			"tablet1": tablet1.URL,
		},
	}

	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	rpcClient := NewRegistryClient(registry)
	coord := NewCoordinator(rpcClient, Config{
		Store: snapStore,
		NowFn:  (&fixedClock{}).Now,
	})

	ctx := context.Background()
	snapID := "e2e-delete-1"
	tabletIDs := []string{"tablet1"}

	// First create a snapshot
	_, err := coord.CreateSnapshot(ctx, snapID, tabletIDs, 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	// Then delete it
	if err := coord.DeleteSnapshot(ctx, snapID); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}

	if !deleteCalled {
		t.Errorf("expected delete to be called on tablet")
	}

	// Verify snapshot is gone
	_, err = coord.GetSnapshot(snapID)
	if err == nil {
		t.Errorf("expected error getting deleted snapshot")
	}
}

// mockTabletRegistry is a test implementation of TabletRPCRegistry.
type mockTabletRegistry struct {
	endpoints map[string]string
}

func (m *mockTabletRegistry) GetEndpoint(tabletID string) (string, error) {
	ep, ok := m.endpoints[tabletID]
	if !ok {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "tablet not found", false, nil)
	}
	return ep, nil
}

// fixedClock returns a fixed time for testing.
type fixedClock struct{}

func (f *fixedClock) Now() time.Time {
	return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
}
