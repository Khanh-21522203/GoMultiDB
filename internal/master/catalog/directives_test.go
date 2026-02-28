package catalog

import "testing"

func TestDirectivePlannerCreateWhenNoReplicas(t *testing.T) {
	p := NewDirectivePlanner(3)
	out := p.PlanForTablet(TabletPlacementView{TabletID: "tablet-a", Replicas: map[string]TabletReplicaStatus{}, Tombstoned: true})
	if len(out) != 1 {
		t.Fatalf("expected 1 directive, got %d", len(out))
	}
	if out[0].Action != DirectiveCreateTablet {
		t.Fatalf("expected create directive, got %s", out[0].Action)
	}
}

func TestDirectivePlannerCreateWhenUnderReplicated(t *testing.T) {
	p := NewDirectivePlanner(3)
	out := p.PlanForTablet(TabletPlacementView{
		TabletID: "tablet-a",
		Replicas: map[string]TabletReplicaStatus{
			"ts-1": {TSUUID: "ts-1", LastSeqNo: 1},
			"ts-2": {TSUUID: "ts-2", LastSeqNo: 1},
		},
	})
	if len(out) != 1 || out[0].Action != DirectiveCreateTablet {
		t.Fatalf("expected create directive for under replication, got %+v", out)
	}
}

func TestDirectivePlannerDeleteWhenOverReplicated(t *testing.T) {
	p := NewDirectivePlanner(2)
	out := p.PlanForTablet(TabletPlacementView{
		TabletID: "tablet-a",
		Replicas: map[string]TabletReplicaStatus{
			"ts-1": {TSUUID: "ts-1", LastSeqNo: 1},
			"ts-2": {TSUUID: "ts-2", LastSeqNo: 1},
			"ts-3": {TSUUID: "ts-3", LastSeqNo: 1},
		},
	})
	if len(out) != 1 || out[0].Action != DirectiveDeleteTablet {
		t.Fatalf("expected delete directive for over replication, got %+v", out)
	}
}

func TestDirectivePlannerNoopWhenBalanced(t *testing.T) {
	p := NewDirectivePlanner(2)
	out := p.PlanForTablet(TabletPlacementView{
		TabletID: "tablet-a",
		Replicas: map[string]TabletReplicaStatus{
			"ts-1": {TSUUID: "ts-1", LastSeqNo: 1},
			"ts-2": {TSUUID: "ts-2", LastSeqNo: 1},
		},
	})
	if len(out) != 0 {
		t.Fatalf("expected no directives, got %+v", out)
	}
}
