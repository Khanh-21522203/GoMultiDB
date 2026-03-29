// Package invariant provides assertion helpers for distributed-system correctness properties.
package invariant

import (
	"fmt"
	"time"
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

// ClusterInspector provides read-only access to cluster state for invariant checking.
// Implementations query the system under test via admin APIs or direct in-process access.
type ClusterInspector interface {
	// ReadKey reads the current value of key from the cluster. Returns (nil, nil) if not found.
	ReadKey(key string) ([]byte, error)
	// TabletReplicaStates returns the last-applied opID for each replica of each tablet.
	TabletReplicaStates() ([]TabletReplicaState, error)
	// ListPendingIntents returns the list of non-terminal transaction IDs with outstanding intents.
	ListPendingIntents() ([]string, error)
	// ListAppliedOpIDs returns the set of operation IDs applied on a given node (for dedup check).
	ListAppliedOpIDs(nodeID string) (map[uint64]int, error) // opID → apply count
}

// AssertNoLostWrites verifies that every acked write is readable from the cluster.
// Returns the first key whose value does not match.
func AssertNoLostWrites(writes []WriteRecord, inspector ClusterInspector) error {
	for _, w := range writes {
		got, err := inspector.ReadKey(w.Key)
		if err != nil {
			return fmt.Errorf("no_lost_writes: ReadKey(%q): %w", w.Key, err)
		}
		if string(got) != string(w.Value) {
			return fmt.Errorf("no_lost_writes: key=%q: want %q, got %q", w.Key, w.Value, got)
		}
	}
	return nil
}

// AssertOwnershipConvergence verifies ownership and replication convergence:
// each tablet must have exactly one primary owner and all replicas should
// converge to the same apply position within tolerance.
func AssertOwnershipConvergence(inspector ClusterInspector, tolerance time.Duration) error {
	states, err := inspector.TabletReplicaStates()
	if err != nil {
		return fmt.Errorf("ownership_convergence: list states: %w", err)
	}

	// Group by tabletID.
	byTablet := make(map[string][]TabletReplicaState)
	for _, s := range states {
		byTablet[s.TabletID] = append(byTablet[s.TabletID], s)
	}

	now := time.Now()
	for tabletID, replicas := range byTablet {
		primaries := 0
		for _, r := range replicas {
			if r.IsPrimary {
				primaries++
			}
		}
		if primaries != 1 {
			return fmt.Errorf("ownership_convergence: tablet=%s expected exactly 1 primary owner, got %d", tabletID, primaries)
		}
		if len(replicas) < 2 {
			continue
		}
		var minOp, maxOp uint64
		minOp = replicas[0].LastOpID
		maxOp = replicas[0].LastOpID
		for _, r := range replicas[1:] {
			if r.LastOpID < minOp {
				minOp = r.LastOpID
			}
			if r.LastOpID > maxOp {
				maxOp = r.LastOpID
			}
		}
		if maxOp != minOp {
			// Check if the lagging replica's state is stale within tolerance.
			for _, r := range replicas {
				if r.LastOpID == minOp && now.Sub(r.Timestamp) > tolerance {
					return fmt.Errorf("ownership_convergence: tablet=%s replica=%s lagging: min=%d max=%d",
						tabletID, r.NodeID, minOp, maxOp)
				}
			}
		}
	}
	return nil
}

// AssertRoutingConsistency verifies that each tablet has one primary route and
// that the primary signal is fresh enough for routing.
func AssertRoutingConsistency(inspector ClusterInspector, primaryStaleness time.Duration) error {
	states, err := inspector.TabletReplicaStates()
	if err != nil {
		return fmt.Errorf("routing_consistency: list states: %w", err)
	}
	byTablet := make(map[string][]TabletReplicaState)
	for _, s := range states {
		byTablet[s.TabletID] = append(byTablet[s.TabletID], s)
	}

	now := time.Now()
	for tabletID, replicas := range byTablet {
		var primary *TabletReplicaState
		primaryCount := 0
		for i := range replicas {
			if !replicas[i].IsPrimary {
				continue
			}
			primaryCount++
			if primary == nil {
				primary = &replicas[i]
			}
		}
		if primaryCount != 1 || primary == nil {
			return fmt.Errorf("routing_consistency: tablet=%s expected exactly 1 primary route, got %d", tabletID, primaryCount)
		}
		if now.Sub(primary.Timestamp) > primaryStaleness {
			return fmt.Errorf("routing_consistency: tablet=%s primary=%s signal stale=%s", tabletID, primary.NodeID, now.Sub(primary.Timestamp))
		}
	}
	return nil
}

// AssertDurabilityNoPendingIntents verifies that no non-terminal transaction intents remain.
func AssertDurabilityNoPendingIntents(inspector ClusterInspector) error {
	pending, err := inspector.ListPendingIntents()
	if err != nil {
		return fmt.Errorf("durability_no_pending_intents: list: %w", err)
	}
	if len(pending) > 0 {
		return fmt.Errorf("durability_no_pending_intents: %d non-terminal intents remain: %v",
			len(pending), pending)
	}
	return nil
}

// AssertDurabilityNoDoubleApply verifies that no operation was applied more than once on any node.
func AssertDurabilityNoDoubleApply(nodeIDs []string, inspector ClusterInspector) error {
	for _, nodeID := range nodeIDs {
		applied, err := inspector.ListAppliedOpIDs(nodeID)
		if err != nil {
			return fmt.Errorf("durability_no_double_apply: list ops for %s: %w", nodeID, err)
		}
		for opID, count := range applied {
			if count > 1 {
				return fmt.Errorf("durability_no_double_apply: node=%s opID=%d applied %d times",
					nodeID, opID, count)
			}
		}
	}
	return nil
}

// AssertReplicaConvergence is kept as a compatibility alias for ownership convergence.
func AssertReplicaConvergence(inspector ClusterInspector, tolerance time.Duration) error {
	return AssertOwnershipConvergence(inspector, tolerance)
}

// AssertNoUncommittedIntents is kept as a compatibility alias for durability checks.
func AssertNoUncommittedIntents(inspector ClusterInspector) error {
	return AssertDurabilityNoPendingIntents(inspector)
}

// AssertNoDoubleApply is kept as a compatibility alias for durability checks.
func AssertNoDoubleApply(nodeIDs []string, inspector ClusterInspector) error {
	return AssertDurabilityNoDoubleApply(nodeIDs, inspector)
}

// AssertInvariant dispatches to the named invariant checker.
// Supported names: "no_lost_writes" (requires writes), "ownership_convergence",
// "routing_consistency", "durability_no_pending_intents",
// "durability_no_double_apply" (requires nodeIDs).
func AssertInvariant(name string, inspector ClusterInspector, opts ...any) error {
	switch name {
	case "ownership_convergence", "replica_convergence":
		tolerance := 5 * time.Second
		for _, o := range opts {
			if d, ok := o.(time.Duration); ok {
				tolerance = d
			}
		}
		return AssertOwnershipConvergence(inspector, tolerance)
	case "routing_consistency":
		staleness := 5 * time.Second
		for _, o := range opts {
			if d, ok := o.(time.Duration); ok {
				staleness = d
			}
		}
		return AssertRoutingConsistency(inspector, staleness)
	case "durability_no_pending_intents", "no_uncommitted_intents":
		return AssertDurabilityNoPendingIntents(inspector)
	case "durability_no_double_apply", "no_double_apply":
		var nodeIDs []string
		for _, o := range opts {
			if ids, ok := o.([]string); ok {
				nodeIDs = ids
			}
		}
		return AssertDurabilityNoDoubleApply(nodeIDs, inspector)
	case "no_lost_writes":
		var writes []WriteRecord
		for _, o := range opts {
			if ws, ok := o.([]WriteRecord); ok {
				writes = ws
			}
		}
		return AssertNoLostWrites(writes, inspector)
	default:
		return fmt.Errorf("unknown invariant: %q", name)
	}
}
