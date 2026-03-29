// Package registry provides a tablet RPC registry backed by the master catalog
// and heartbeat system.
package registry

import (
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestTabletRPCRegistry_GetEndpoint(t *testing.T) {
	tsManager := &mockTSManager{
		descriptors: map[string]TSDescriptor{
			"ts-1": {
				Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
				Registration: TSRegistration{RPCAddress: "ts-1.example.com:9100"},
			},
		},
	}
	sink := &mockReconcileSink{
		tablets: map[string]TabletPlacementView{
			"tablet-1": {
				TabletID: "tablet-1",
				Replicas: map[string]TabletReplicaStatus{
					"ts-1": {TSUUID: "ts-1", LastSeqNo: 100},
				},
				PrimaryTSUUID: "ts-1",
				Tombstoned:    false,
			},
		},
	}

	registry := NewTabletRPCRegistry(tsManager, sink)

	endpoint, err := registry.GetEndpoint("tablet-1")
	if err != nil {
		t.Fatalf("get endpoint: %v", err)
	}
	if endpoint != "http://ts-1.example.com:9100" {
		t.Errorf("expected endpoint 'http://ts-1.example.com:9100', got '%s'", endpoint)
	}
}

func TestTabletRPCRegistry_TabletNotFound(t *testing.T) {
	tsManager := &mockTSManager{descriptors: make(map[string]TSDescriptor)}
	sink := &mockReconcileSink{tablets: make(map[string]TabletPlacementView)}

	registry := NewTabletRPCRegistry(tsManager, sink)

	_, err := registry.GetEndpoint("unknown-tablet")
	if err == nil {
		t.Fatalf("expected error for unknown tablet")
	}
}

func TestTabletRPCRegistry_TombstonedTablet(t *testing.T) {
	tsManager := &mockTSManager{descriptors: make(map[string]TSDescriptor)}
	sink := &mockReconcileSink{
		tablets: map[string]TabletPlacementView{
			"tablet-1": {
				TabletID:   "tablet-1",
				Replicas:   map[string]TabletReplicaStatus{},
				Tombstoned: true,
			},
		},
	}

	registry := NewTabletRPCRegistry(tsManager, sink)

	_, err := registry.GetEndpoint("tablet-1")
	if err == nil {
		t.Fatalf("expected error for tombstoned tablet")
	}
}

func TestTabletRPCRegistry_NoAvailableReplicas(t *testing.T) {
	tsManager := &mockTSManager{descriptors: make(map[string]TSDescriptor)}
	sink := &mockReconcileSink{
		tablets: map[string]TabletPlacementView{
			"tablet-1": {
				TabletID:   "tablet-1",
				Replicas:   map[string]TabletReplicaStatus{},
				Tombstoned: false,
			},
		},
	}

	registry := NewTabletRPCRegistry(tsManager, sink)

	_, err := registry.GetEndpoint("tablet-1")
	if err == nil {
		t.Fatalf("expected error for tablet with no replicas")
	}
}

func TestTabletRPCRegistry_SelectsPrimaryOwner(t *testing.T) {
	tsManager := &mockTSManager{
		descriptors: map[string]TSDescriptor{
			"ts-1": {
				Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
				Registration: TSRegistration{RPCAddress: "ts-1.example.com:9100"},
			},
			"ts-2": {
				Instance:     TSInstance{PermanentUUID: "ts-2", InstanceSeqNo: 1},
				Registration: TSRegistration{RPCAddress: "ts-2.example.com:9100"},
			},
		},
	}
	sink := &mockReconcileSink{
		tablets: map[string]TabletPlacementView{
			"tablet-1": {
				TabletID: "tablet-1",
				Replicas: map[string]TabletReplicaStatus{
					"ts-1": {TSUUID: "ts-1", LastSeqNo: 100},
					"ts-2": {TSUUID: "ts-2", LastSeqNo: 101},
				},
				PrimaryTSUUID: "ts-2",
				Tombstoned:    false,
			},
		},
	}

	registry := NewTabletRPCRegistry(tsManager, sink)

	endpoint, err := registry.GetEndpoint("tablet-1")
	if err != nil {
		t.Fatalf("get endpoint: %v", err)
	}
	if endpoint != "http://ts-2.example.com:9100" {
		t.Errorf("expected primary endpoint ts-2, got '%s'", endpoint)
	}
}

func TestTabletRPCRegistry_DeterministicFallbackWithoutPrimaryMetadata(t *testing.T) {
	tsManager := &mockTSManager{
		descriptors: map[string]TSDescriptor{
			"ts-a": {
				Instance:     TSInstance{PermanentUUID: "ts-a", InstanceSeqNo: 1},
				Registration: TSRegistration{RPCAddress: "ts-a.example.com:9100"},
			},
			"ts-b": {
				Instance:     TSInstance{PermanentUUID: "ts-b", InstanceSeqNo: 1},
				Registration: TSRegistration{RPCAddress: "ts-b.example.com:9100"},
			},
		},
	}
	sink := &mockReconcileSink{
		tablets: map[string]TabletPlacementView{
			"tablet-1": {
				TabletID: "tablet-1",
				Replicas: map[string]TabletReplicaStatus{
					"ts-b": {TSUUID: "ts-b", LastSeqNo: 9},
					"ts-a": {TSUUID: "ts-a", LastSeqNo: 9},
				},
			},
		},
	}
	registry := NewTabletRPCRegistry(tsManager, sink)
	endpoint, err := registry.GetEndpoint("tablet-1")
	if err != nil {
		t.Fatalf("get endpoint: %v", err)
	}
	if endpoint != "http://ts-a.example.com:9100" {
		t.Fatalf("expected deterministic fallback ts-a, got %s", endpoint)
	}
}

func TestTabletRPCRegistry_PrimaryOwnerChangedError(t *testing.T) {
	tsManager := &mockTSManager{
		descriptors: map[string]TSDescriptor{
			"ts-1": {
				Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
				Registration: TSRegistration{RPCAddress: "ts-1.example.com:9100"},
			},
		},
	}
	sink := &mockReconcileSink{
		tablets: map[string]TabletPlacementView{
			"tablet-1": {
				TabletID:      "tablet-1",
				PrimaryTSUUID: "ts-2",
				Replicas: map[string]TabletReplicaStatus{
					"ts-1": {TSUUID: "ts-1", LastSeqNo: 100},
				},
			},
		},
	}
	registry := NewTabletRPCRegistry(tsManager, sink)
	_, err := registry.GetEndpoint("tablet-1")
	if err == nil {
		t.Fatalf("expected error")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrPrimaryOwnerChanged {
		t.Fatalf("expected ErrPrimaryOwnerChanged, got %s", n.Code)
	}
}

// Mock implementations for testing

type mockTSManager struct {
	descriptors map[string]TSDescriptor
}

func (m *mockTSManager) Get(uuid string) (TSDescriptor, bool) {
	d, ok := m.descriptors[uuid]
	return d, ok
}

type mockReconcileSink struct {
	tablets map[string]TabletPlacementView
}

func (m *mockReconcileSink) GetTablet(tabletID string) (TabletPlacementView, bool) {
	v, ok := m.tablets[tabletID]
	if !ok {
		return TabletPlacementView{}, false
	}
	// Return a copy to avoid race conditions
	replicas := make(map[string]TabletReplicaStatus, len(v.Replicas))
	for k, v := range v.Replicas {
		replicas[k] = v
	}
	return TabletPlacementView{
		TabletID:      v.TabletID,
		Replicas:      replicas,
		PrimaryTSUUID: v.PrimaryTSUUID,
		Tombstoned:    v.Tombstoned,
		LastUpdated:   v.LastUpdated,
	}, true
}
