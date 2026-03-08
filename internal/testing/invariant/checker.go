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

// TabletState represents the last-applied operation ID for a replica.
type TabletReplicaState struct {
	NodeID    string
	TabletID  string
	LastOpID  uint64
	Timestamp time.Time
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

// AssertReplicaConvergence verifies that all replicas of each tablet have the same
// last-applied opID within the given tolerance window.
func AssertReplicaConvergence(inspector ClusterInspector, tolerance time.Duration) error {
	states, err := inspector.TabletReplicaStates()
	if err != nil {
		return fmt.Errorf("replica_convergence: list states: %w", err)
	}

	// Group by tabletID.
	byTablet := make(map[string][]TabletReplicaState)
	for _, s := range states {
		byTablet[s.TabletID] = append(byTablet[s.TabletID], s)
	}

	now := time.Now()
	for tabletID, replicas := range byTablet {
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
					return fmt.Errorf("replica_convergence: tablet=%s replica=%s lagging: min=%d max=%d",
						tabletID, r.NodeID, minOp, maxOp)
				}
			}
		}
	}
	return nil
}

// AssertNoUncommittedIntents verifies that no non-terminal transaction intents remain.
func AssertNoUncommittedIntents(inspector ClusterInspector) error {
	pending, err := inspector.ListPendingIntents()
	if err != nil {
		return fmt.Errorf("no_uncommitted_intents: list: %w", err)
	}
	if len(pending) > 0 {
		return fmt.Errorf("no_uncommitted_intents: %d non-terminal intents remain: %v",
			len(pending), pending)
	}
	return nil
}

// AssertNoDoubleApply verifies that no operation was applied more than once on any node.
func AssertNoDoubleApply(nodeIDs []string, inspector ClusterInspector) error {
	for _, nodeID := range nodeIDs {
		applied, err := inspector.ListAppliedOpIDs(nodeID)
		if err != nil {
			return fmt.Errorf("no_double_apply: list ops for %s: %w", nodeID, err)
		}
		for opID, count := range applied {
			if count > 1 {
				return fmt.Errorf("no_double_apply: node=%s opID=%d applied %d times",
					nodeID, opID, count)
			}
		}
	}
	return nil
}

// AssertInvariant dispatches to the named invariant checker.
// Supported names: "no_lost_writes" (requires writes), "replica_convergence",
// "no_uncommitted_intents", "no_double_apply" (requires nodeIDs).
func AssertInvariant(name string, inspector ClusterInspector, opts ...any) error {
	switch name {
	case "replica_convergence":
		tolerance := 5 * time.Second
		for _, o := range opts {
			if d, ok := o.(time.Duration); ok {
				tolerance = d
			}
		}
		return AssertReplicaConvergence(inspector, tolerance)
	case "no_uncommitted_intents":
		return AssertNoUncommittedIntents(inspector)
	case "no_double_apply":
		var nodeIDs []string
		for _, o := range opts {
			if ids, ok := o.([]string); ok {
				nodeIDs = ids
			}
		}
		return AssertNoDoubleApply(nodeIDs, inspector)
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
