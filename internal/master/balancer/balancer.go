// Package balancer implements the load-balancing and replica-placement planner
// for the GoMultiDB master server.
//
// Design (docs/plans/plan-load-balancing-and-replica-placement.md):
//
//	PlanBalanceRound takes a ClusterState snapshot and returns a list of
//	BalanceActions to bring the cluster closer to the desired state.
//
// Priority order (per plan):
//  1. Under-replicated tablets  → add_replica  (highest priority)
//  2. Over-replicated tablets   → remove_replica
//  3. Leader imbalance          → move_leader  (if LeaderBalancingEnabled)
//
// Actions per round are capped by the per-type concurrency limits.
// A cooldown map prevents re-moving the same tablet within CooldownWindow.
package balancer

import (
	"sort"
	"sync"
	"time"
)

// ── Data Structures ───────────────────────────────────────────────────────────

// NodeLoad is the observed state of a single tablet server.
type NodeLoad struct {
	NodeID       string
	ReplicaCount int
	LeaderCount  int
	IsLive       bool
	// Placement is a rack/zone label used to enforce diverse replica placement.
	Placement string
}

// ReplicaPlacement records which node holds a replica and whether it is leader.
type ReplicaPlacement struct {
	NodeID   string
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
	// Type is one of "add_replica", "remove_replica", "move_leader".
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

// ── Config ────────────────────────────────────────────────────────────────────

// Config controls planner behaviour.
type Config struct {
	// MaxConcurrentAdds is the maximum add_replica actions per round.
	MaxConcurrentAdds int
	// MaxConcurrentRemovals is the maximum remove_replica actions per round.
	MaxConcurrentRemovals int
	// MaxConcurrentLeaderMoves is the maximum move_leader actions per round.
	MaxConcurrentLeaderMoves int
	// LeaderBalancingEnabled enables leader-count balancing (Priority 3).
	LeaderBalancingEnabled bool
	// CooldownWindow prevents re-actioning the same tablet within this duration.
	// Zero disables cooldown.
	CooldownWindow time.Duration
	// NowFn returns the current time (injectable for tests).
	NowFn func() time.Time
}

func (c *Config) defaults() {
	if c.MaxConcurrentAdds <= 0 {
		c.MaxConcurrentAdds = 5
	}
	if c.MaxConcurrentRemovals <= 0 {
		c.MaxConcurrentRemovals = 5
	}
	if c.MaxConcurrentLeaderMoves <= 0 {
		c.MaxConcurrentLeaderMoves = 5
	}
	if c.CooldownWindow <= 0 {
		c.CooldownWindow = 30 * time.Second
	}
	if c.NowFn == nil {
		c.NowFn = func() time.Time { return time.Now().UTC() }
	}
}

// ── Planner ───────────────────────────────────────────────────────────────────

// Planner is stateful (owns the cooldown map) and goroutine-safe.
type Planner struct {
	mu       sync.Mutex
	cfg      Config
	cooldown map[string]time.Time // tabletID → last action time
}

// NewPlanner creates a Planner with the given config.
func NewPlanner(cfg Config) *Planner {
	cfg.defaults()
	return &Planner{
		cfg:      cfg,
		cooldown: make(map[string]time.Time),
	}
}

// PlanBalanceRound inspects the ClusterState and returns actions to improve balance.
// It is safe to call concurrently.
func (p *Planner) PlanBalanceRound(state ClusterState) ([]BalanceAction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.cfg.NowFn()
	liveNodes := liveNodeMap(state.Nodes)
	nodeLoad := loadIndex(state.Nodes)

	var actions []BalanceAction
	adds, rems, moves := 0, 0, 0

	// Helper: emit action if budget allows.
	emit := func(a BalanceAction) bool {
		switch a.Type {
		case "add_replica":
			if adds >= p.cfg.MaxConcurrentAdds {
				return false
			}
			adds++
		case "remove_replica":
			if rems >= p.cfg.MaxConcurrentRemovals {
				return false
			}
			rems++
		case "move_leader":
			if moves >= p.cfg.MaxConcurrentLeaderMoves {
				return false
			}
			moves++
		}
		actions = append(actions, a)
		p.cooldown[a.TabletID] = now
		return true
	}

	// ── Priority 1: Under-replicated ─────────────────────────────────────────
	for _, tab := range state.Tablets {
		if inCooldown(p.cooldown, tab.TabletID, now, p.cfg.CooldownWindow) {
			continue
		}
		have := liveReplicaCount(tab, liveNodes)
		if have >= tab.RF {
			continue
		}
		// Find the least-loaded live node not already holding a replica,
		// whose placement differs from existing replicas (rack awareness).
		candidate := pickAddNode(tab, state.Nodes, nodeLoad, liveNodes)
		if candidate == "" {
			continue // no suitable node
		}
		if !emit(BalanceAction{
			Type:     "add_replica",
			TabletID: tab.TabletID,
			ToNode:   candidate,
			Reason:   "under-replicated",
		}) {
			break
		}
		// Update nodeLoad optimistically so subsequent tablets use updated counts.
		if nl, ok := nodeLoad[candidate]; ok {
			nl.ReplicaCount++
			nodeLoad[candidate] = nl
		}
	}

	// ── Priority 2: Over-replicated ──────────────────────────────────────────
	for _, tab := range state.Tablets {
		if inCooldown(p.cooldown, tab.TabletID, now, p.cfg.CooldownWindow) {
			continue
		}
		have := liveReplicaCount(tab, liveNodes)
		if have <= tab.RF {
			continue
		}
		// Remove from the most-loaded non-leader node.
		victim := pickRemoveNode(tab, nodeLoad, liveNodes)
		if victim == "" {
			continue
		}
		if !emit(BalanceAction{
			Type:     "remove_replica",
			TabletID: tab.TabletID,
			FromNode: victim,
			Reason:   "over-replicated",
		}) {
			break
		}
		if nl, ok := nodeLoad[victim]; ok {
			nl.ReplicaCount--
			nodeLoad[victim] = nl
		}
	}

	// ── Priority 3: Leader imbalance ─────────────────────────────────────────
	if p.cfg.LeaderBalancingEnabled {
		meanLeaders := meanLeaderCount(state.Nodes, liveNodes)
		for _, tab := range state.Tablets {
			if inCooldown(p.cooldown, tab.TabletID, now, p.cfg.CooldownWindow) {
				continue
			}
			from, to := pickLeaderMove(tab, nodeLoad, liveNodes, meanLeaders)
			if from == "" || to == "" {
				continue
			}
			if !emit(BalanceAction{
				Type:     "move_leader",
				TabletID: tab.TabletID,
				FromNode: from,
				ToNode:   to,
				Reason:   "leader imbalance",
			}) {
				break
			}
			if nl, ok := nodeLoad[from]; ok {
				nl.LeaderCount--
				nodeLoad[from] = nl
			}
			if nl, ok := nodeLoad[to]; ok {
				nl.LeaderCount++
				nodeLoad[to] = nl
			}
		}
	}

	return actions, nil
}

// NotifyActionResult updates the cooldown map.
// Call with success=false to reset the cooldown so the next round can retry.
func (p *Planner) NotifyActionResult(tabletID string, success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !success {
		delete(p.cooldown, tabletID)
	}
}

// GetPlacementViolations returns tablets that cannot satisfy their RF due to
// insufficient distinct placement labels or insufficient live nodes.
func GetPlacementViolations(state ClusterState) []Violation {
	liveNodes := liveNodeMap(state.Nodes)
	var violations []Violation
	for _, tab := range state.Tablets {
		liveCount := liveReplicaCount(tab, liveNodes)
		if liveCount < tab.RF {
			// Check if there are enough live nodes at all.
			if len(liveNodes) < tab.RF {
				violations = append(violations, Violation{
					TabletID: tab.TabletID,
					Reason:   "insufficient live nodes to satisfy RF",
				})
				continue
			}
			// Check distinct placement labels.
			labels := placementsAvailable(state.Nodes, liveNodes)
			if labels < tab.RF {
				violations = append(violations, Violation{
					TabletID: tab.TabletID,
					Reason:   "insufficient distinct placement zones to satisfy rack-aware RF",
				})
			}
		}
	}
	return violations
}

// ── internal helpers ──────────────────────────────────────────────────────────

func liveNodeMap(nodes []NodeLoad) map[string]bool {
	m := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if n.IsLive {
			m[n.NodeID] = true
		}
	}
	return m
}

func loadIndex(nodes []NodeLoad) map[string]NodeLoad {
	m := make(map[string]NodeLoad, len(nodes))
	for _, n := range nodes {
		m[n.NodeID] = n
	}
	return m
}

func liveReplicaCount(tab TabletPlacement, live map[string]bool) int {
	count := 0
	for _, r := range tab.Replicas {
		if live[r.NodeID] {
			count++
		}
	}
	return count
}

func inCooldown(cd map[string]time.Time, tabletID string, now time.Time, window time.Duration) bool {
	if window <= 0 {
		return false
	}
	last, ok := cd[tabletID]
	return ok && now.Sub(last) < window
}

// pickAddNode returns the ID of the best node to add a replica to:
// must be live, not already hosting a replica, and (if possible) have a
// distinct placement label from existing replicas.
func pickAddNode(
	tab TabletPlacement,
	nodes []NodeLoad,
	nodeLoad map[string]NodeLoad,
	live map[string]bool,
) string {
	existing := make(map[string]bool, len(tab.Replicas))
	existingPlacements := make(map[string]bool)
	for _, r := range tab.Replicas {
		existing[r.NodeID] = true
		if nl, ok := nodeLoad[r.NodeID]; ok {
			existingPlacements[nl.Placement] = true
		}
	}

	// Prefer nodes with a new placement label; fall back to any valid node.
	var candidates []NodeLoad
	for _, n := range nodes {
		if !live[n.NodeID] || existing[n.NodeID] {
			continue
		}
		candidates = append(candidates, nodeLoad[n.NodeID])
	}
	if len(candidates) == 0 {
		return ""
	}

	// Sort: prefer distinct placement, then fewest replicas.
	sort.Slice(candidates, func(i, j int) bool {
		iNew := !existingPlacements[candidates[i].Placement]
		jNew := !existingPlacements[candidates[j].Placement]
		if iNew != jNew {
			return iNew // prefer new placement
		}
		return candidates[i].ReplicaCount < candidates[j].ReplicaCount
	})
	return candidates[0].NodeID
}

// pickRemoveNode returns the node to remove a replica from:
// prefer the most-loaded non-leader node that holds a live replica.
func pickRemoveNode(
	tab TabletPlacement,
	nodeLoad map[string]NodeLoad,
	live map[string]bool,
) string {
	type candidate struct {
		nodeID       string
		replicaCount int
		isLeader     bool
	}
	var candidates []candidate
	for _, r := range tab.Replicas {
		if !live[r.NodeID] {
			continue
		}
		nl := nodeLoad[r.NodeID]
		candidates = append(candidates, candidate{r.NodeID, nl.ReplicaCount, r.IsLeader})
	}
	if len(candidates) == 0 {
		return ""
	}
	// Sort: non-leaders first, then by descending replica count.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].isLeader != candidates[j].isLeader {
			return !candidates[i].isLeader
		}
		return candidates[i].replicaCount > candidates[j].replicaCount
	})
	return candidates[0].nodeID
}

// meanLeaderCount returns the mean leader count across live nodes.
func meanLeaderCount(nodes []NodeLoad, live map[string]bool) float64 {
	total, count := 0, 0
	for _, n := range nodes {
		if live[n.NodeID] {
			total += n.LeaderCount
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}

// pickLeaderMove returns (from, to) nodes for a leader move on the given tablet.
// from must be the current leader; to must be a replica on a less-loaded node.
func pickLeaderMove(
	tab TabletPlacement,
	nodeLoad map[string]NodeLoad,
	live map[string]bool,
	mean float64,
) (from, to string) {
	var leaderNode string
	var followerCandidates []NodeLoad
	for _, r := range tab.Replicas {
		if !live[r.NodeID] {
			continue
		}
		nl := nodeLoad[r.NodeID]
		if r.IsLeader {
			leaderNode = r.NodeID
		} else {
			followerCandidates = append(followerCandidates, nl)
		}
	}
	if leaderNode == "" {
		return "", ""
	}
	leaderLoad := nodeLoad[leaderNode]
	if float64(leaderLoad.LeaderCount) <= mean {
		return "", "" // leader is not overloaded
	}
	// Pick follower with lowest leader count that is below mean.
	sort.Slice(followerCandidates, func(i, j int) bool {
		return followerCandidates[i].LeaderCount < followerCandidates[j].LeaderCount
	})
	for _, fc := range followerCandidates {
		if float64(fc.LeaderCount) < mean {
			return leaderNode, fc.NodeID
		}
	}
	return "", ""
}

// placementsAvailable counts distinct placement labels among live nodes.
func placementsAvailable(nodes []NodeLoad, live map[string]bool) int {
	labels := make(map[string]bool)
	for _, n := range nodes {
		if live[n.NodeID] {
			labels[n.Placement] = true
		}
	}
	return len(labels)
}
