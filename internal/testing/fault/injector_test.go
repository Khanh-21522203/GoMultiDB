package fault_test

import (
	"testing"
	"time"

	"GoMultiDB/internal/testing/fault"
)

func TestFaultInjectorInjectAndHeal(t *testing.T) {
	fi := fault.NewFaultInjector()

	action, err := fi.InjectFault(fault.FaultAction{
		Type:     fault.FaultPartition,
		FromNode: "node-1",
		ToNode:   "node-2",
	})
	if err != nil {
		t.Fatalf("InjectFault: %v", err)
	}
	if action.FaultID == "" {
		t.Fatalf("expected auto-assigned FaultID")
	}
	if !fi.IsPartitioned("node-1", "node-2") {
		t.Fatalf("expected partition to be active")
	}

	if err := fi.HealFault(action.FaultID); err != nil {
		t.Fatalf("HealFault: %v", err)
	}
	if fi.IsPartitioned("node-1", "node-2") {
		t.Fatalf("partition should be healed")
	}
}

func TestFaultInjectorPartitionBidirectional(t *testing.T) {
	fi := fault.NewFaultInjector()
	_, _ = fi.InjectFault(fault.FaultAction{
		Type:     fault.FaultPartition,
		FromNode: "a",
		ToNode:   "b",
	})
	// Should match in both directions.
	if !fi.IsPartitioned("a", "b") {
		t.Fatalf("a→b should be partitioned")
	}
	if !fi.IsPartitioned("b", "a") {
		t.Fatalf("b→a should also be partitioned")
	}
}

func TestFaultInjectorHealAll(t *testing.T) {
	fi := fault.NewFaultInjector()
	_, _ = fi.InjectFault(fault.FaultAction{Type: fault.FaultPartition, FromNode: "x", ToNode: "y"})
	_, _ = fi.InjectFault(fault.FaultAction{Type: fault.FaultKill, ToNode: "z"})
	fi.HealAll()
	if len(fi.ActiveFaults()) != 0 {
		t.Fatalf("expected no active faults after HealAll")
	}
}

func TestFaultInjectorDelayFor(t *testing.T) {
	fi := fault.NewFaultInjector()
	_, _ = fi.InjectFault(fault.FaultAction{
		Type:     fault.FaultDelay,
		FromNode: "a",
		ToNode:   "b",
		Value:    int64(50 * time.Millisecond),
	})
	d := fi.DelayFor("a", "b")
	if d != 50*time.Millisecond {
		t.Fatalf("delay: want 50ms, got %v", d)
	}
	if fi.DelayFor("b", "a") != 0 {
		t.Fatalf("delay should be directional")
	}
}

func TestFaultInjectorIsKilled(t *testing.T) {
	fi := fault.NewFaultInjector()
	_, _ = fi.InjectFault(fault.FaultAction{Type: fault.FaultKill, ToNode: "node-3"})
	if !fi.IsKilled("node-3") {
		t.Fatalf("node-3 should be killed")
	}
	if fi.IsKilled("node-4") {
		t.Fatalf("node-4 should not be killed")
	}
}

func TestFaultInjectorIsDiskFull(t *testing.T) {
	fi := fault.NewFaultInjector()
	_, _ = fi.InjectFault(fault.FaultAction{Type: fault.FaultDiskFull, ToNode: "node-5"})
	if !fi.IsDiskFull("node-5") {
		t.Fatalf("node-5 should have disk full fault")
	}
}

func TestFaultInjectorClockSkewFor(t *testing.T) {
	fi := fault.NewFaultInjector()
	_, _ = fi.InjectFault(fault.FaultAction{
		Type:    fault.FaultClockSkew,
		ToNode:  "node-6",
		Value:   int64(200 * time.Millisecond),
	})
	skew := fi.ClockSkewFor("node-6")
	if skew != 200*time.Millisecond {
		t.Fatalf("clock skew: want 200ms, got %v", skew)
	}
}

func TestFaultInjectorHealNotFound(t *testing.T) {
	fi := fault.NewFaultInjector()
	if err := fi.HealFault("nonexistent"); err == nil {
		t.Fatalf("expected error when healing nonexistent fault")
	}
}

func TestFaultInjectorExplicitFaultID(t *testing.T) {
	fi := fault.NewFaultInjector()
	action, err := fi.InjectFault(fault.FaultAction{
		FaultID: "my-fault-1",
		Type:    fault.FaultKill,
		ToNode:  "x",
	})
	if err != nil {
		t.Fatalf("InjectFault: %v", err)
	}
	if action.FaultID != "my-fault-1" {
		t.Fatalf("expected preserved FaultID, got %s", action.FaultID)
	}
}

func TestFaultInjectorActiveFaultsSnapshot(t *testing.T) {
	fi := fault.NewFaultInjector()
	_, _ = fi.InjectFault(fault.FaultAction{Type: fault.FaultPartition, FromNode: "a", ToNode: "b"})
	_, _ = fi.InjectFault(fault.FaultAction{Type: fault.FaultKill, ToNode: "c"})
	faults := fi.ActiveFaults()
	if len(faults) != 2 {
		t.Fatalf("expected 2 active faults, got %d", len(faults))
	}
}
