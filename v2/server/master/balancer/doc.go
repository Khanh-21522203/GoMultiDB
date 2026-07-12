// Package balancer implements the load-balancing and replica-placement
// planner for the GoMultiDB master server: Planner takes a ClusterState
// snapshot (node loads and tablet placements) and returns a prioritized
// list of BalanceActions (add_replica, remove_replica, transfer_primary)
// to bring the cluster closer to its desired replication factor and
// primary-ownership balance, subject to per-round concurrency limits and a
// per-tablet cooldown. GetPlacementViolations reports tablets that cannot
// satisfy their replication factor given current node liveness and
// placement diversity. It is consumed by v2/server (the master runtime's
// periodic balancing loop).
// This is scaffold-only; behavior is unimplemented.
package balancer
