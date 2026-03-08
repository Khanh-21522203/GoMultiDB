package platform

import (
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestMemTrackerConsumeAndRelease(t *testing.T) {
	mt := NewMemTracker(1000)
	if err := mt.Consume(300); err != nil {
		t.Fatalf("Consume 300: %v", err)
	}
	if got := mt.CurrentUsage(); got != 300 {
		t.Fatalf("usage: want 300, got %d", got)
	}
	mt.Release(100)
	if got := mt.CurrentUsage(); got != 200 {
		t.Fatalf("usage after release: want 200, got %d", got)
	}
}

func TestMemTrackerHardLimitEnforced(t *testing.T) {
	mt := NewMemTracker(500)
	if err := mt.Consume(500); err != nil {
		t.Fatalf("Consume up to limit: %v", err)
	}
	err := mt.Consume(1)
	if err == nil {
		t.Fatalf("expected ErrOOMKill when exceeding limit")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrOOMKill {
		t.Fatalf("expected ErrOOMKill, got %s", n.Code)
	}
}

func TestMemTrackerNoLimitWhenZero(t *testing.T) {
	mt := NewMemTracker(0)
	if err := mt.Consume(1 << 30); err != nil {
		t.Fatalf("unlimited tracker should not fail: %v", err)
	}
}

func TestMemTrackerChildPropagates(t *testing.T) {
	root := NewMemTracker(1000)
	child := root.NewChild("raft-log")

	if err := child.Consume(400); err != nil {
		t.Fatalf("child Consume: %v", err)
	}
	if got := root.CurrentUsage(); got != 400 {
		t.Fatalf("root usage after child consume: want 400, got %d", got)
	}
	if got := child.CurrentUsage(); got != 400 {
		t.Fatalf("child usage: want 400, got %d", got)
	}
}

func TestMemTrackerChildReleasesPropagates(t *testing.T) {
	root := NewMemTracker(1000)
	child := root.NewChild("docdb-memtable")
	_ = child.Consume(300)
	child.Release(100)
	if got := root.CurrentUsage(); got != 200 {
		t.Fatalf("root usage after child release: want 200, got %d", got)
	}
}

func TestMemTrackerChildLimitRespected(t *testing.T) {
	root := NewMemTracker(500)
	child := root.NewChild("pggate-buffer")
	_ = child.Consume(400)

	// Next consume should breach root limit
	err := child.Consume(200)
	if err == nil {
		t.Fatalf("expected OOM error from child consume over root limit")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrOOMKill {
		t.Fatalf("expected ErrOOMKill, got %s", n.Code)
	}
}

func TestMemTrackerChildIdempotentNew(t *testing.T) {
	root := NewMemTracker(1000)
	c1 := root.NewChild("cdc-event-store")
	c2 := root.NewChild("cdc-event-store")
	if c1 != c2 {
		t.Fatalf("NewChild should return same instance for same name")
	}
}

func TestMemTrackerName(t *testing.T) {
	root := NewMemTracker(1000)
	if root.Name() != "root" {
		t.Fatalf("root name: want root, got %s", root.Name())
	}
	child := root.NewChild("raft-log")
	if child.Name() != "raft-log" {
		t.Fatalf("child name: want raft-log, got %s", child.Name())
	}
}
