package cdc

import (
	"context"
	"testing"
)

func TestEvaluateRetention(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()
	if err := svc.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	status, err := svc.EvaluateRetention(ctx, "s1", 10, 8)
	if err != nil {
		t.Fatalf("evaluate retention: %v", err)
	}
	if status != RetentionOK {
		t.Fatalf("expected retention OK, got %s", status)
	}

	status, err = svc.EvaluateRetention(ctx, "s1", 7, 8)
	if err != nil {
		t.Fatalf("evaluate retention bootstrap-needed: %v", err)
	}
	if status != RetentionBootstrapNeeded {
		t.Fatalf("expected bootstrap-needed, got %s", status)
	}
}

func TestRegisterSplitRemap(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()
	if err := svc.CreateStream(ctx, "s1", "parent"); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	if err := svc.RegisterSplitRemap(ctx, "s1", SplitRemap{ParentTabletID: "parent", ChildTabletIDs: []string{"child-2", "child-1"}}); err != nil {
		t.Fatalf("register split remap: %v", err)
	}

	out, err := svc.GetChanges(ctx, GetChangesRequest{StreamID: "s1"})
	if err != nil {
		t.Fatalf("get changes after remap: %v", err)
	}
	if out.TabletID != "child-1" {
		t.Fatalf("expected stream tablet remapped to sorted first child, got %s", out.TabletID)
	}

	children, err := svc.SplitChildren(ctx, "s1")
	if err != nil {
		t.Fatalf("split children: %v", err)
	}
	if len(children) != 2 || children[0] != "child-1" || children[1] != "child-2" {
		t.Fatalf("unexpected children list: %+v", children)
	}
}
