// Package snapshot provides the RPC service for tablet snapshot operations.
package snapshot

import (
	"context"
	"encoding/json"
	"testing"

	"GoMultiDB/internal/common/types"
	"GoMultiDB/internal/storage/rocks"
)

func TestService_CreateTabletSnapshot(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	svc := NewService(store)

	req := CreateTabletSnapshotRequest{
		SnapshotID: "snap1",
		TabletID:   "tablet1",
		CreateHT:   100,
	}
	payload, _ := json.Marshal(req)

	respBytes, err := svc.Methods()["create_tablet_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("create_tablet_snapshot rpc: %v", err)
	}

	var resp CreateTabletSnapshotResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.SnapshotID != "snap1" {
		t.Errorf("expected snapshot id snap1, got %s", resp.SnapshotID)
	}
}

func TestService_DeleteTabletSnapshot(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	svc := NewService(store)
	ctx := context.Background()

	// First create a snapshot
	_, err := store.CreateSnapshot(ctx, "snap1", "tablet1", 100)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	// Delete via RPC
	req := DeleteTabletSnapshotRequest{
		SnapshotID: "snap1",
		TabletID:   "tablet1",
	}
	payload, _ := json.Marshal(req)

	_, err = svc.Methods()["delete_tablet_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("delete_tablet_snapshot rpc: %v", err)
	}

	// Verify it's deleted
	_, err = store.GetSnapshot(ctx, "snap1")
	if err == nil {
		t.Fatalf("expected error getting deleted snapshot")
	}
}

func TestService_RestoreTabletSnapshot(t *testing.T) {
	rs := rocks.NewMemoryStore()
	store := NewStore(rs)
	svc := NewService(store)
	ctx := context.Background()

	// Write test data
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

	// Modify data
	wb2 := rocks.WriteBatch{
		Ops: []rocks.KV{
			{Key: []byte("key1"), Value: []byte("value2")},
		},
	}
	if err := rs.ApplyWriteBatch(ctx, rocks.RegularDB, wb2); err != nil {
		t.Fatalf("modify data: %v", err)
	}

	// Restore via RPC
	req := RestoreTabletSnapshotRequest{
		SnapshotID: "snap1",
		TabletID:   "tablet1",
	}
	payload, _ := json.Marshal(req)

	_, err = svc.Methods()["restore_tablet_snapshot"](context.Background(), types.RequestEnvelope{}, payload)
	if err != nil {
		t.Fatalf("restore_tablet_snapshot rpc: %v", err)
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

func TestService_InvalidJSON(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	svc := NewService(store)

	methods := []string{"create_tablet_snapshot", "delete_tablet_snapshot", "restore_tablet_snapshot"}
	for _, method := range methods {
		_, err := svc.Methods()[method](context.Background(), types.RequestEnvelope{}, []byte("invalid json"))
		if err == nil {
			t.Fatalf("%s: expected error for invalid json", method)
		}
	}
}

func TestService_Name(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	svc := NewService(store)

	if svc.Name() != "tablet_snapshot" {
		t.Errorf("expected service name 'tablet_snapshot', got %s", svc.Name())
	}
}

func TestService_Methods(t *testing.T) {
	store := NewStore(rocks.NewMemoryStore())
	svc := NewService(store)

	methods := svc.Methods()
	expectedMethods := []string{
		"create_tablet_snapshot",
		"delete_tablet_snapshot",
		"restore_tablet_snapshot",
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
