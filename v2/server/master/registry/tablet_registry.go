package registry

import (
	errors "GoMultiDB/v2/contracts/errors"
)

// TSManager is the interface for accessing tablet server descriptors.
type TSManager interface {
	// Get returns the TSDescriptor registered under uuid, and whether it
	// was found.
	Get(uuid string) (TSDescriptor, bool)
}

// TSDescriptor represents a registered tablet server.
type TSDescriptor struct {
	Instance        TSInstance
	Registration    TSRegistration
	LastHeartbeatAt interface{}
}

// TSInstance identifies a tablet server instance.
type TSInstance struct {
	PermanentUUID string
	InstanceSeqNo uint64
}

// TSRegistration contains the tablet server's network addresses.
type TSRegistration struct {
	RPCAddress  string
	HTTPAddress string
}

// ReconcileSink is the interface for accessing tablet placement
// information.
type ReconcileSink interface {
	// GetTablet returns the TabletPlacementView for tabletID, and whether
	// it was found.
	GetTablet(tabletID string) (TabletPlacementView, bool)
}

// TabletPlacementView describes where a tablet is hosted.
type TabletPlacementView struct {
	TabletID      string
	Replicas      map[string]TabletReplicaStatus
	PrimaryTSUUID string
	Tombstoned    bool
	LastUpdated   uint64
}

// TabletReplicaStatus describes a replica on a specific tablet server.
type TabletReplicaStatus struct {
	TSUUID    string
	LastSeqNo uint64
}

// TabletRPCRegistry looks up tablet RPC endpoints from the master's
// catalog and heartbeat data.
//
// Scaffold stub: GetEndpoint returns errors.ErrNotImplemented.
type TabletRPCRegistry struct {
	tsManager TSManager
	sink      ReconcileSink
}

// NewTabletRPCRegistry creates a new registry that looks up tablet
// endpoints by combining placement information (from a ReconcileSink)
// with tablet server registration (from a TSManager).
func NewTabletRPCRegistry(tsManager TSManager, sink ReconcileSink) *TabletRPCRegistry {
	return &TabletRPCRegistry{}
}

// GetEndpoint returns the RPC endpoint for the tablet's primary owner. It
// prefers explicit PrimaryTSUUID metadata and falls back to deterministic
// selection by sequence number when metadata is missing.
//
// Not yet implemented in this scaffold.
func (r *TabletRPCRegistry) GetEndpoint(tabletID string) (string, error) {
	return "", errors.ErrNotImplemented
}
