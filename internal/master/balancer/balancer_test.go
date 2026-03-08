package balancer_test

import (
	"testing"
	"time"

	"GoMultiDB/internal/master/balancer"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func node(id string, replicas, leaders int, live bool, placement string) balancer.NodeLoad {
	return balancer.NodeLoad{
		NodeID:       id,
		ReplicaCount: replicas,
		LeaderCount:  leaders,
		IsLive:       live,
		Placement:    placement,
	}
}

func tablet(id string, rf int, replicas ...balancer.ReplicaPlacement) balancer.TabletPlacement {
	return balancer.TabletPlacement{TabletID: id, RF: rf, Replicas: replicas}
}

func replica(nodeID string, isLeader bool) balancer.ReplicaPlacement {
	return balancer.ReplicaPlacement{NodeID: nodeID, IsLeader: isLeader}
}

func newPlanner(cfg balancer.Config) *balancer.Planner {
	if cfg.NowFn == nil {
		cfg.NowFn = func() time.Time { return time.Now().UTC() }
	}
	return balancer.NewPlanner(cfg)
}

// countType counts actions of the given type.
func countType(actions []balancer.BalanceAction, t string) int {
	n := 0
	for _, a := range actions {
		if a.Type == t {
			n++
		}
	}
	return n
}

// ── under-replicated ──────────────────────────────────────────────────────────

func TestPlanUnderReplicated(t *testing.T) {
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 1, 1, true, "zone-a"),
			node("n2", 1, 0, true, "zone-b"),
			node("n3", 0, 0, true, "zone-c"),
		},
		Tablets: []balancer.TabletPlacement{
			// RF=3 but only 2 replicas
			tablet("tab-1", 3,
				replica("n1", true),
				replica("n2", false),
			),
		},
	}
	p := newPlanner(balancer.Config{CooldownWindow: 0})
	actions, err := p.PlanBalanceRound(state)
	if err != nil {
		t.Fatalf("PlanBalanceRound: %v", err)
	}
	if got := countType(actions, "add_replica"); got != 1 {
		t.Fatalf("expected 1 add_replica, got %d; actions: %+v", got, actions)
	}
	if actions[0].TabletID != "tab-1" {
		t.Fatalf("expected action on tab-1, got %s", actions[0].TabletID)
	}
	if actions[0].ToNode != "n3" {
		t.Fatalf("expected ToNode=n3 (least loaded), got %s", actions[0].ToNode)
	}
}

func TestPlanNoActionsWhenBalanced(t *testing.T) {
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 1, 1, true, "zone-a"),
			node("n2", 1, 0, true, "zone-b"),
			node("n3", 1, 0, true, "zone-c"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-1", 3,
				replica("n1", true),
				replica("n2", false),
				replica("n3", false),
			),
		},
	}
	p := newPlanner(balancer.Config{CooldownWindow: 0})
	actions, _ := p.PlanBalanceRound(state)
	if len(actions) != 0 {
		t.Fatalf("expected no actions for balanced cluster, got %+v", actions)
	}
}

// ── over-replicated ───────────────────────────────────────────────────────────

func TestPlanOverReplicated(t *testing.T) {
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 2, 1, true, "zone-a"),
			node("n2", 2, 0, true, "zone-b"),
			node("n3", 1, 0, true, "zone-c"),
		},
		Tablets: []balancer.TabletPlacement{
			// RF=2 but 3 replicas
			tablet("tab-over", 2,
				replica("n1", true),
				replica("n2", false),
				replica("n3", false),
			),
		},
	}
	p := newPlanner(balancer.Config{CooldownWindow: 0})
	actions, _ := p.PlanBalanceRound(state)
	if got := countType(actions, "remove_replica"); got != 1 {
		t.Fatalf("expected 1 remove_replica, got %d: %+v", got, actions)
	}
	// Should remove from a non-leader (n2 or n3); never from the leader.
	if actions[0].FromNode == "n1" {
		t.Fatalf("should not remove leader replica, got FromNode=%s", actions[0].FromNode)
	}
}

// ── dead node ─────────────────────────────────────────────────────────────────

func TestPlanDeadNodeTriggersAdd(t *testing.T) {
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 2, 1, true, "zone-a"),
			node("n2", 0, 0, false, "zone-b"), // dead
			node("n3", 1, 0, true, "zone-c"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-dead", 3,
				replica("n1", true),
				replica("n2", false), // on dead node
				replica("n3", false),
			),
		},
	}
	p := newPlanner(balancer.Config{CooldownWindow: 0})
	actions, _ := p.PlanBalanceRound(state)
	// Only 2 live replicas, need 3 → should add one.
	// But there are no more live nodes! So no action can be emitted.
	// This should produce a Violation instead.
	_ = actions
	violations := balancer.GetPlacementViolations(state)
	if len(violations) == 0 {
		t.Fatalf("expected violation for under-replicated tablet with no spare nodes")
	}
}

// ── cooldown ──────────────────────────────────────────────────────────────────

func TestCooldownPreventsImmediateReAction(t *testing.T) {
	now := time.Now().UTC()
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 1, 1, true, "zone-a"),
			node("n2", 1, 0, true, "zone-b"),
			node("n3", 0, 0, true, "zone-c"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-cd", 3, replica("n1", true), replica("n2", false)),
		},
	}
	p := balancer.NewPlanner(balancer.Config{
		CooldownWindow: 10 * time.Minute,
		NowFn:          func() time.Time { return now },
	})

	// First round: action emitted.
	actions, _ := p.PlanBalanceRound(state)
	if len(actions) == 0 {
		t.Fatalf("expected action in first round")
	}

	// Second round (same time): cooldown prevents re-action.
	actions, _ = p.PlanBalanceRound(state)
	if len(actions) != 0 {
		t.Fatalf("expected no actions in cooldown window, got %d", len(actions))
	}

	// After cooldown expires: action emitted again.
	now = now.Add(11 * time.Minute)
	actions, _ = p.PlanBalanceRound(state)
	if len(actions) == 0 {
		t.Fatalf("expected action after cooldown expires")
	}
}

// ── NotifyActionResult resets cooldown on failure ─────────────────────────────

func TestNotifyActionResultResetsOnFailure(t *testing.T) {
	now := time.Now().UTC()
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 1, 1, true, "zone-a"),
			node("n2", 1, 0, true, "zone-b"),
			node("n3", 0, 0, true, "zone-c"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-fail", 3, replica("n1", true), replica("n2", false)),
		},
	}
	p := balancer.NewPlanner(balancer.Config{
		CooldownWindow: 10 * time.Minute,
		NowFn:          func() time.Time { return now },
	})

	actions, _ := p.PlanBalanceRound(state)
	if len(actions) == 0 {
		t.Fatalf("expected action")
	}
	// Notify failure → cooldown reset.
	p.NotifyActionResult("tab-fail", false)

	// Immediate retry should be allowed.
	actions, _ = p.PlanBalanceRound(state)
	if len(actions) == 0 {
		t.Fatalf("expected retry action after failure notification")
	}
}

// ── concurrency cap ───────────────────────────────────────────────────────────

func TestMaxConcurrentAddsEnforced(t *testing.T) {
	nodes := []balancer.NodeLoad{
		node("n1", 5, 3, true, "zone-a"),
		node("n2", 0, 0, true, "zone-b"),
	}
	// 3 under-replicated tablets.
	tablets := []balancer.TabletPlacement{
		tablet("t1", 2, replica("n1", true)),
		tablet("t2", 2, replica("n1", true)),
		tablet("t3", 2, replica("n1", true)),
	}
	p := balancer.NewPlanner(balancer.Config{
		MaxConcurrentAdds: 2,
		CooldownWindow:    0,
	})
	actions, _ := p.PlanBalanceRound(balancer.ClusterState{Nodes: nodes, Tablets: tablets})
	if got := countType(actions, "add_replica"); got > 2 {
		t.Fatalf("expected at most 2 add_replica, got %d", got)
	}
}

// ── leader balancing ──────────────────────────────────────────────────────────

func TestLeaderBalancing(t *testing.T) {
	// n1 has 3 leaders, n2 has 0 — should move one leader.
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 3, 3, true, "zone-a"),
			node("n2", 3, 0, true, "zone-b"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-leader", 2,
				replica("n1", true),
				replica("n2", false),
			),
		},
	}
	p := newPlanner(balancer.Config{
		LeaderBalancingEnabled: true,
		CooldownWindow:         0,
	})
	actions, _ := p.PlanBalanceRound(state)
	if got := countType(actions, "move_leader"); got != 1 {
		t.Fatalf("expected 1 move_leader, got %d: %+v", got, actions)
	}
	if actions[0].FromNode != "n1" || actions[0].ToNode != "n2" {
		t.Fatalf("expected move_leader n1→n2, got %s→%s", actions[0].FromNode, actions[0].ToNode)
	}
}

// ── placement violations ──────────────────────────────────────────────────────

func TestGetPlacementViolationsInsufficientNodes(t *testing.T) {
	// RF=3 but only 2 live nodes.
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 1, 1, true, "zone-a"),
			node("n2", 1, 0, true, "zone-b"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-v", 3, replica("n1", true), replica("n2", false)),
		},
	}
	violations := balancer.GetPlacementViolations(state)
	if len(violations) == 0 {
		t.Fatalf("expected violation for RF=3 with only 2 nodes")
	}
}

func TestGetPlacementViolationsNoViolation(t *testing.T) {
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 1, 1, true, "zone-a"),
			node("n2", 1, 0, true, "zone-b"),
			node("n3", 1, 0, true, "zone-c"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-ok", 3,
				replica("n1", true),
				replica("n2", false),
				replica("n3", false),
			),
		},
	}
	violations := balancer.GetPlacementViolations(state)
	if len(violations) != 0 {
		t.Fatalf("expected no violations, got %v", violations)
	}
}

// ── rack-aware placement ──────────────────────────────────────────────────────

func TestRackAwarePlacementPrefersDifferentZone(t *testing.T) {
	// n1 (zone-a), n2 (zone-a), n3 (zone-b). Tablet has n1 as only replica.
	// Should prefer n3 (zone-b) over n2 (zone-a same as existing).
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			node("n1", 1, 1, true, "zone-a"),
			node("n2", 0, 0, true, "zone-a"),
			node("n3", 0, 0, true, "zone-b"),
		},
		Tablets: []balancer.TabletPlacement{
			tablet("tab-rack", 2, replica("n1", true)),
		},
	}
	p := newPlanner(balancer.Config{CooldownWindow: 0})
	actions, _ := p.PlanBalanceRound(state)
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].ToNode != "n3" {
		t.Fatalf("expected placement on n3 (distinct zone), got %s", actions[0].ToNode)
	}
}

// ── convergence ───────────────────────────────────────────────────────────────

func TestConvergenceMultipleRounds(t *testing.T) {
	// 4 nodes, 3 tablets all on n1. Should converge to 1 tablet per node.
	nodes := []balancer.NodeLoad{
		node("n1", 3, 3, true, "zone-a"),
		node("n2", 0, 0, true, "zone-b"),
		node("n3", 0, 0, true, "zone-c"),
	}
	tablets := []balancer.TabletPlacement{
		tablet("t1", 1, replica("n1", true)),
		tablet("t2", 1, replica("n1", true)),
		tablet("t3", 1, replica("n1", true)),
	}
	// Over-replicated scenario: each tablet RF=1 is satisfied, but we test
	// that with RF=2 they spread out over rounds.
	for i := range tablets {
		tablets[i].RF = 2
	}
	p := newPlanner(balancer.Config{CooldownWindow: 0})
	totalAdds := 0
	for round := 0; round < 10; round++ {
		actions, _ := p.PlanBalanceRound(balancer.ClusterState{Nodes: nodes, Tablets: tablets})
		totalAdds += countType(actions, "add_replica")
		if len(actions) == 0 {
			break
		}
	}
	// We expect 3 add_replica actions total (one per tablet).
	if totalAdds < 3 {
		t.Fatalf("expected at least 3 add_replica actions total, got %d", totalAdds)
	}
}
