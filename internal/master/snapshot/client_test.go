// Package snapshot provides the RPC client for calling tablet snapshot operations.
package snapshot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestClient_CreateTabletSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/rpc" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var req tabletSnapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Service != "tablet_snapshot" {
			t.Errorf("expected service tablet_snapshot, got %s", req.Service)
		}
		if req.Method != "create_tablet_snapshot" {
			t.Errorf("expected method create_tablet_snapshot, got %s", req.Method)
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(tabletSnapshotResponse{})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.CreateTabletSnapshot(context.Background(), "snap1", "tablet1")
	if err != nil {
		t.Fatalf("create tablet snapshot: %v", err)
	}
}

func TestClient_DeleteTabletSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req tabletSnapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Method != "delete_tablet_snapshot" {
			t.Errorf("expected method delete_tablet_snapshot, got %s", req.Method)
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(tabletSnapshotResponse{})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.DeleteTabletSnapshot(context.Background(), "snap1", "tablet1")
	if err != nil {
		t.Fatalf("delete tablet snapshot: %v", err)
	}
}

func TestClient_RestoreTabletSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req tabletSnapshotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if req.Method != "restore_tablet_snapshot" {
			t.Errorf("expected method restore_tablet_snapshot, got %s", req.Method)
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(tabletSnapshotResponse{})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.RestoreTabletSnapshot(context.Background(), "snap1", "tablet1")
	if err != nil {
		t.Fatalf("restore tablet snapshot: %v", err)
	}
}

func TestClient_RPCError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		errResp := dberrors.New(dberrors.ErrInternal, "tablet error", false, nil)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(tabletSnapshotResponse{Error: &errResp})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.CreateTabletSnapshot(context.Background(), "snap1", "tablet1")
	if err == nil {
		t.Fatalf("expected error for RPC error response")
	}
}

func TestClient_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	err := client.CreateTabletSnapshot(context.Background(), "snap1", "tablet1")
	if err == nil {
		t.Fatalf("expected error for HTTP error")
	}
}

func TestRegistryClient(t *testing.T) {
	registry := &mockRegistry{endpoints: map[string]string{
		"tablet1": "http://tablet1:9100",
		"tablet2": "http://tablet2:9100",
	}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(tabletSnapshotResponse{})
	}))
	defer server.Close()

	// Override the endpoint for testing
	registry.endpoints["tablet1"] = server.URL

	client := NewRegistryClient(registry)
	err := client.CreateTabletSnapshot(context.Background(), "snap1", "tablet1")
	if err != nil {
		t.Fatalf("create tablet snapshot via registry: %v", err)
	}
}

func TestRegistryClient_TabletNotFound(t *testing.T) {
	registry := &mockRegistry{endpoints: map[string]string{}}
	client := NewRegistryClient(registry)

	err := client.CreateTabletSnapshot(context.Background(), "snap1", "unknown")
	if err == nil {
		t.Fatalf("expected error for unknown tablet")
	}
}

// mock registry for testing

type mockRegistry struct {
	endpoints map[string]string
}

func (m *mockRegistry) GetEndpoint(tabletID string) (string, error) {
	ep, ok := m.endpoints[tabletID]
	if !ok {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "tablet not found", false, nil)
	}
	return ep, nil
}
