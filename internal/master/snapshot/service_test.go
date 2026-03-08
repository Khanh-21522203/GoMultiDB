// Package snapshot provides the RPC service for distributed snapshot operations.
package snapshot

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"GoMultiDB/internal/common/types"
	"GoMultiDB/internal/storage/rocks"
)

func TestService_CreateSnapshot(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)

	req := CreateSnapshotRequest{
		SnapshotID: "test-snapshot-1",
		TabletIDs:  []string{"tablet-1", "tablet-2"},
		CreateHT:   12345,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	respBytes, err := svc.Methods()["create_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("create_snapshot rpc: %v", err)
	}

	var resp CreateSnapshotResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.SnapshotInfo == nil {
		t.Fatalf("expected snapshot info in response")
	}
	if resp.SnapshotInfo.SnapshotID != req.SnapshotID {
		t.Fatalf("expected snapshot id %s, got %s", req.SnapshotID, resp.SnapshotInfo.SnapshotID)
	}
	if resp.SnapshotInfo.State != StateComplete {
		t.Fatalf("expected state COMPLETE, got %s", resp.SnapshotInfo.State)
	}
}

func TestService_GetSnapshot(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)

	// First create a snapshot
	createReq := CreateSnapshotRequest{
		SnapshotID: "test-snapshot-2",
		TabletIDs:  []string{"tablet-a"},
		CreateHT:   999,
	}
	payload, _ := json.Marshal(createReq)
	_, err := svc.Methods()["create_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("create_snapshot rpc: %v", err)
	}

	// Now get it
	getReq := GetSnapshotRequest{SnapshotID: "test-snapshot-2"}
	payload, err = json.Marshal(getReq)
	if err != nil {
		t.Fatalf("marshal get request: %v", err)
	}

	respBytes, err := svc.Methods()["get_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("get_snapshot rpc: %v", err)
	}

	var resp GetSnapshotResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.SnapshotInfo == nil {
		t.Fatalf("expected snapshot info in response")
	}
	if resp.SnapshotInfo.SnapshotID != "test-snapshot-2" {
		t.Fatalf("expected snapshot id test-snapshot-2, got %s", resp.SnapshotInfo.SnapshotID)
	}
}

func TestService_ListSnapshots(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)

	// Create two snapshots
	for i := 1; i <= 2; i++ {
		req := CreateSnapshotRequest{
			SnapshotID: "list-test-" + string(rune('0'+i)),
			TabletIDs:  []string{"tablet-1"},
			CreateHT:   uint64(i),
		}
		payload, _ := json.Marshal(req)
		_, err := svc.Methods()["create_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
		if err != nil {
			t.Fatalf("create_snapshot rpc %d: %v", i, err)
		}
	}

	// List snapshots
	listReq := ListSnapshotsRequest{}
	payload, _ := json.Marshal(listReq)
	respBytes, err := svc.Methods()["list_snapshots"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("list_snapshots rpc: %v", err)
	}

	var resp ListSnapshotsResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if len(resp.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(resp.Snapshots))
	}
}

func TestService_DeleteSnapshot(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)

	// Create a snapshot
	createReq := CreateSnapshotRequest{
		SnapshotID: "to-delete",
		TabletIDs:  []string{"tablet-1"},
		CreateHT:   111,
	}
	payload, _ := json.Marshal(createReq)
	_, err := svc.Methods()["create_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("create_snapshot rpc: %v", err)
	}

	// Delete it
	deleteReq := DeleteSnapshotRequest{SnapshotID: "to-delete"}
	payload, err = json.Marshal(deleteReq)
	if err != nil {
		t.Fatalf("marshal delete request: %v", err)
	}

	_, err = svc.Methods()["delete_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("delete_snapshot rpc: %v", err)
	}

	// Verify it's gone
	getReq := GetSnapshotRequest{SnapshotID: "to-delete"}
	payload, _ = json.Marshal(getReq)
	_, err = svc.Methods()["get_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err == nil {
		t.Fatalf("expected error getting deleted snapshot")
	}
}

func TestService_RestoreSnapshot(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)

	// Create a snapshot
	createReq := CreateSnapshotRequest{
		SnapshotID: "to-restore",
		TabletIDs:  []string{"tablet-1"},
		CreateHT:   222,
	}
	payload, _ := json.Marshal(createReq)
	_, err := svc.Methods()["create_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("create_snapshot rpc: %v", err)
	}

	// Restore it
	restoreReq := RestoreSnapshotRequest{SnapshotID: "to-restore"}
	payload, err = json.Marshal(restoreReq)
	if err != nil {
		t.Fatalf("marshal restore request: %v", err)
	}

	_, err = svc.Methods()["restore_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("restore_snapshot rpc: %v", err)
	}

	// Verify it's still COMPLETE
	getReq := GetSnapshotRequest{SnapshotID: "to-restore"}
	payload, _ = json.Marshal(getReq)
	respBytes, err := svc.Methods()["get_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("get_snapshot after restore: %v", err)
	}

	var resp GetSnapshotResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.SnapshotInfo.State != StateComplete {
		t.Fatalf("expected state COMPLETE after restore, got %s", resp.SnapshotInfo.State)
	}
}

func TestService_InvalidJSON(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)

	// Test invalid JSON for each method
	methods := []string{"create_snapshot", "delete_snapshot", "restore_snapshot", "get_snapshot", "list_snapshots"}
	for _, method := range methods {
		_, err := svc.Methods()[method](context.Background(), types.RequestEnvelope{}, []byte("invalid json"))
		if err == nil {
			t.Fatalf("%s: expected error for invalid json", method)
		}
	}
}

func TestService_Name(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)
	if svc.Name() != "snapshot" {
		t.Fatalf("expected service name 'snapshot', got %s", svc.Name())
	}
}

func TestService_Methods(t *testing.T) {
	rocksStore := rocks.NewMemoryStore()
	snapStore := NewRocksSnapshotStore(rocksStore)
	coord := NewCoordinator(&noopTabletSnapshotRPC{}, Config{
		Store: snapStore,
		NowFn:  time.Now().UTC,
	})

	svc := NewService(coord)
	methods := svc.Methods()

	expectedMethods := []string{
		"create_snapshot",
		"delete_snapshot",
		"restore_snapshot",
		"get_snapshot",
		"list_snapshots",
	}

	if len(methods) != len(expectedMethods) {
		t.Fatalf("expected %d methods, got %d", len(expectedMethods), len(methods))
	}

	for _, name := range expectedMethods {
		if _, ok := methods[name]; !ok {
			t.Fatalf("missing method: %s", name)
		}
	}
}
