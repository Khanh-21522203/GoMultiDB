package cdc

import (
	"context"
	"testing"
)

func TestBootstrapLifecycle(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()
	if err := svc.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	status, err := svc.BootstrapStatus(ctx, "s1")
	if err != nil {
		t.Fatalf("bootstrap status none: %v", err)
	}
	if status.State != BootstrapStateNone {
		t.Fatalf("expected NONE, got %s", status.State)
	}

	if err := svc.StartBootstrap(ctx, "s1"); err != nil {
		t.Fatalf("start bootstrap: %v", err)
	}
	status, _ = svc.BootstrapStatus(ctx, "s1")
	if status.State != BootstrapStateInProgress {
		t.Fatalf("expected IN_PROGRESS, got %s", status.State)
	}

	if err := svc.CompleteBootstrap(ctx, "s1"); err != nil {
		t.Fatalf("complete bootstrap: %v", err)
	}
	status, _ = svc.BootstrapStatus(ctx, "s1")
	if status.State != BootstrapStateComplete {
		t.Fatalf("expected COMPLETE, got %s", status.State)
	}

	if err := svc.FailBootstrap(ctx, "s1", "example failure"); err != nil {
		t.Fatalf("fail bootstrap: %v", err)
	}
	status, _ = svc.BootstrapStatus(ctx, "s1")
	if status.State != BootstrapStateFailed {
		t.Fatalf("expected FAILED, got %s", status.State)
	}
}

func TestEvaluateRetentionAndMarkBootstrap(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()
	if err := svc.CreateStream(ctx, "s2", "t2"); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	status, err := svc.EvaluateRetentionAndMark(ctx, "s2", 5, 6)
	if err != nil {
		t.Fatalf("evaluate retention and mark: %v", err)
	}
	if status != RetentionBootstrapNeeded {
		t.Fatalf("expected bootstrap needed, got %s", status)
	}
	bs, err := svc.BootstrapStatus(ctx, "s2")
	if err != nil {
		t.Fatalf("bootstrap status: %v", err)
	}
	if bs.State != BootstrapStateRequired {
		t.Fatalf("expected REQUIRED state, got %s", bs.State)
	}
}
