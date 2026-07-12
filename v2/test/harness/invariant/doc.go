// Package invariant provides assertion helpers for distributed-system
// correctness properties: AssertNoLostWrites, AssertOwnershipConvergence,
// AssertRoutingConsistency, AssertDurabilityNoPendingIntents, and
// AssertDurabilityNoDoubleApply each check one property against a
// ClusterInspector — the read-only seam a caller implements to expose
// cluster state — and AssertInvariant dispatches to one of them by name.
// It is consumed by v2/test/integration and v2/test/stress tests verifying
// cluster correctness after fault injection or sustained load.
// This is scaffold-only; behavior is unimplemented.
package invariant
