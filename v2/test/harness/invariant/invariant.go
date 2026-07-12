package invariant

import (
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// WriteRecord represents a write that was acknowledged by the cluster.
type WriteRecord struct {
	Key   string
	Value []byte
}

// TabletReplicaState represents per-node apply state for a tablet.
type TabletReplicaState struct {
	NodeID    string
	TabletID  string
	LastOpID  uint64
	Timestamp time.Time
	// IsPrimary marks the node currently serving primary ownership/routing.
	IsPrimary bool
}

// ClusterInspector provides read-only access to cluster state for
// invariant checking. Implementations query the system under test via
// admin APIs or direct in-process access.
type ClusterInspector interface {
	// ReadKey reads the current value of key from the cluster. Returns
	// (nil, nil) if not found.
	ReadKey(key string) ([]byte, error)
	// TabletReplicaStates returns the last-applied opID for each replica of
	// each tablet.
	TabletReplicaStates() ([]TabletReplicaState, error)
	// ListPendingIntents returns the list of non-terminal transaction IDs
	// with outstanding intents.
	ListPendingIntents() ([]string, error)
	// ListAppliedOpIDs returns the set of operation IDs applied on a given
	// node (for dedup check).
	ListAppliedOpIDs(nodeID string) (map[uint64]int, error) // opID → apply count
}

// AssertNoLostWrites verifies that every acked write is readable from the
// cluster. Returns the first key whose value does not match.
//
// Not yet implemented in this scaffold.
func AssertNoLostWrites(writes []WriteRecord, inspector ClusterInspector) error {
	return errors.ErrNotImplemented
}

// AssertOwnershipConvergence verifies ownership and replication
// convergence: each tablet must have exactly one primary owner and all
// replicas should converge to the same apply position within tolerance.
//
// Not yet implemented in this scaffold.
func AssertOwnershipConvergence(inspector ClusterInspector, tolerance time.Duration) error {
	return errors.ErrNotImplemented
}

// AssertRoutingConsistency verifies that each tablet has one primary route
// and that the primary signal is fresh enough for routing.
//
// Not yet implemented in this scaffold.
func AssertRoutingConsistency(inspector ClusterInspector, primaryStaleness time.Duration) error {
	return errors.ErrNotImplemented
}

// AssertDurabilityNoPendingIntents verifies that no non-terminal
// transaction intents remain.
//
// Not yet implemented in this scaffold.
func AssertDurabilityNoPendingIntents(inspector ClusterInspector) error {
	return errors.ErrNotImplemented
}

// AssertDurabilityNoDoubleApply verifies that no operation was applied more
// than once on any node.
//
// Not yet implemented in this scaffold.
func AssertDurabilityNoDoubleApply(nodeIDs []string, inspector ClusterInspector) error {
	return errors.ErrNotImplemented
}

// AssertReplicaConvergence is kept as a compatibility alias for ownership
// convergence.
//
// Not yet implemented in this scaffold.
func AssertReplicaConvergence(inspector ClusterInspector, tolerance time.Duration) error {
	return errors.ErrNotImplemented
}

// AssertNoUncommittedIntents is kept as a compatibility alias for
// durability checks.
//
// Not yet implemented in this scaffold.
func AssertNoUncommittedIntents(inspector ClusterInspector) error {
	return errors.ErrNotImplemented
}

// AssertNoDoubleApply is kept as a compatibility alias for durability
// checks.
//
// Not yet implemented in this scaffold.
func AssertNoDoubleApply(nodeIDs []string, inspector ClusterInspector) error {
	return errors.ErrNotImplemented
}

// AssertInvariant dispatches to the named invariant checker. Supported
// names: "no_lost_writes" (requires writes), "ownership_convergence",
// "routing_consistency", "durability_no_pending_intents",
// "durability_no_double_apply" (requires nodeIDs).
//
// Not yet implemented in this scaffold.
func AssertInvariant(name string, inspector ClusterInspector, opts ...any) error {
	return errors.ErrNotImplemented
}
