// Package registry provides a tablet RPC registry backed by the master catalog
// and heartbeat system.
package registry

import (
	"fmt"

	dberrors "GoMultiDB/internal/common/errors"
)

// TSManager is the interface for accessing tablet server descriptors.
type TSManager interface {
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

// ReconcileSink is the interface for accessing tablet placement information.
type ReconcileSink interface {
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

// TabletRPCRegistry looks up tablet RPC endpoints from the master's catalog
// and heartbeat data.
type TabletRPCRegistry struct {
	tsManager TSManager
	sink      ReconcileSink
}

// NewTabletRPCRegistry creates a new registry that looks up tablet endpoints
// by combining placement information (from reconcile sink) with
// tablet server registration (from TSManager).
func NewTabletRPCRegistry(tsManager TSManager, sink ReconcileSink) *TabletRPCRegistry {
	return &TabletRPCRegistry{
		tsManager: tsManager,
		sink:      sink,
	}
}

// GetEndpoint returns the RPC endpoint for the tablet's primary owner.
// It prefers explicit PrimaryTSUUID metadata and falls back to deterministic
// selection by sequence number when metadata is missing.
func (r *TabletRPCRegistry) GetEndpoint(tabletID string) (string, error) {
	if r.sink == nil {
		return "", dberrors.New(dberrors.ErrInternal, "reconcile sink not configured", false, nil)
	}
	if r.tsManager == nil {
		return "", dberrors.New(dberrors.ErrInternal, "ts manager not configured", false, nil)
	}

	// Get tablet placement view from reconcile sink
	view, ok := r.sink.GetTablet(tabletID)
	if !ok {
		return "", dberrors.New(dberrors.ErrInvalidArgument, fmt.Sprintf("tablet not found: %s", tabletID), false, nil)
	}

	// Check if tablet is tombstoned
	if view.Tombstoned {
		return "", dberrors.New(dberrors.ErrConflict, fmt.Sprintf("tablet is tombstoned: %s", tabletID), true, nil)
	}

	primary := view.PrimaryTSUUID
	if primary == "" {
		primary = selectDeterministicPrimary(view.Replicas)
	}
	if primary == "" {
		return "", dberrors.New(dberrors.ErrRetryableUnavailable, fmt.Sprintf("no available replica for tablet: %s", tabletID), true, nil)
	}
	if _, ok := view.Replicas[primary]; !ok {
		return "", dberrors.New(
			dberrors.ErrPrimaryOwnerChanged,
			fmt.Sprintf("primary owner is stale for tablet %s; refresh endpoint metadata", tabletID),
			true,
			nil,
		)
	}

	desc, ok := r.tsManager.Get(primary)
	if !ok {
		return "", dberrors.New(
			dberrors.ErrPrimaryOwnerChanged,
			fmt.Sprintf("primary owner %s is unavailable for tablet %s; refresh endpoint metadata", primary, tabletID),
			true,
			nil,
		)
	}
	return fmt.Sprintf("http://%s", desc.Registration.RPCAddress), nil
}

func selectDeterministicPrimary(replicas map[string]TabletReplicaStatus) string {
	primary := ""
	var maxSeq uint64
	for tsUUID, rep := range replicas {
		if primary == "" || rep.LastSeqNo > maxSeq || (rep.LastSeqNo == maxSeq && tsUUID < primary) {
			primary = tsUUID
			maxSeq = rep.LastSeqNo
		}
	}
	return primary
}
