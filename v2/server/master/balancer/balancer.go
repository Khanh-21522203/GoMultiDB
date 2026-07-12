package balancer

import (
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// NodeLoad is the observed state of a single tablet server.
type NodeLoad struct {
	NodeID       string
	ReplicaCount int
	// LeaderCount is interpreted as primary-owner count in post-Raft mode.
	LeaderCount int
	IsLive      bool
	// Placement is a rack/zone label used to enforce diverse replica
	// placement.
	Placement string
}

// ReplicaPlacement records which node holds a replica and whether it is
// primary.
type ReplicaPlacement struct {
	NodeID string
	// IsLeader is retained for compatibility; it represents primary
	// ownership.
	IsLeader bool
}

// TabletPlacement is the current placement for one tablet.
type TabletPlacement struct {
	TabletID string
	Replicas []ReplicaPlacement
	// RF is the desired replication factor.
	RF int
}

// BalanceAction is a directive emitted by the planner.
type BalanceAction struct {
	// Type is one of "add_replica", "remove_replica", "transfer_primary".
	Type     string
	TabletID string
	FromNode string // empty for add_replica
	ToNode   string // empty for remove_replica
	Reason   string
}

// ClusterState is a point-in-time snapshot of all nodes and tablets.
type ClusterState struct {
	Nodes   []NodeLoad
	Tablets []TabletPlacement
}

// Violation records a placement constraint that could not be satisfied.
type Violation struct {
	TabletID string
	Reason   string
}

// Config controls planner behaviour.
type Config struct {
	// MaxConcurrentAdds is the maximum add_replica actions per round.
	MaxConcurrentAdds int
	// MaxConcurrentRemovals is the maximum remove_replica actions per
	// round.
	MaxConcurrentRemovals int
	// MaxConcurrentLeaderMoves is deprecated. Use
	// MaxConcurrentPrimaryTransfers.
	MaxConcurrentLeaderMoves int
	// MaxConcurrentPrimaryTransfers is the maximum transfer_primary
	// actions per round.
	MaxConcurrentPrimaryTransfers int
	// LeaderBalancingEnabled is deprecated. Use PrimaryBalancingEnabled.
	LeaderBalancingEnabled bool
	// PrimaryBalancingEnabled enables primary-owner balancing.
	PrimaryBalancingEnabled bool
	// CooldownWindow prevents re-actioning the same tablet within this
	// duration. Zero disables cooldown.
	CooldownWindow time.Duration
	// NowFn returns the current time (injectable for tests).
	NowFn func() time.Time
}

// Planner plans corrective balance actions for a ClusterState. It is
// stateful (it owns a per-tablet cooldown map) and safe for concurrent
// use.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Planner struct {
	mu       sync.Mutex
	cfg      Config
	cooldown map[string]time.Time // tabletID -> last action time
}

// NewPlanner creates a Planner with the given config, applying defaults
// for any unset fields.
func NewPlanner(cfg Config) *Planner {
	return &Planner{}
}

// PlanBalanceRound inspects state and returns actions to improve balance.
// It is safe to call concurrently.
//
// Not yet implemented in this scaffold.
func (p *Planner) PlanBalanceRound(state ClusterState) ([]BalanceAction, error) {
	return nil, errors.ErrNotImplemented
}

// NotifyActionResult updates the cooldown map. Call with success=false to
// reset the cooldown so the next round can retry.
//
// Not yet implemented in this scaffold.
func (p *Planner) NotifyActionResult(tabletID string, success bool) {
	panic("not implemented")
}

// GetPlacementViolations returns tablets that cannot satisfy their
// replication factor due to insufficient distinct placement labels or
// insufficient live nodes.
//
// Not yet implemented in this scaffold.
func GetPlacementViolations(state ClusterState) []Violation {
	panic("not implemented")
}
