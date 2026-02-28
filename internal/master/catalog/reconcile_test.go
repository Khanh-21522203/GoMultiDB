package catalog

import (
	"context"
	"testing"
)

func TestMemoryReconcileSinkTracksUpdatedReplicas(t *testing.T) {
	sink := NewMemoryReconcileSink()
	err := sink.ApplyTabletReport(context.Background(), TabletReportDelta{
		TSUUID:        "ts-1",
		IsIncremental: false,
		SequenceNo:    5,
		Updated:       []string{"tablet-a"},
	})
	if err != nil {
		t.Fatalf("apply report: %v", err)
	}
	v, ok := sink.GetTablet("tablet-a")
	if !ok {
		t.Fatalf("expected tablet view to exist")
	}
	if v.Tombstoned {
		t.Fatalf("expected tablet not tombstoned")
	}
	rep, ok := v.Replicas["ts-1"]
	if !ok {
		t.Fatalf("expected replica for ts-1")
	}
	if rep.LastSeqNo != 5 {
		t.Fatalf("unexpected replica seq: %d", rep.LastSeqNo)
	}
}

func TestMemoryReconcileSinkTracksRemovedReplicaAndTombstone(t *testing.T) {
	sink := NewMemoryReconcileSink()
	if err := sink.ApplyTabletReport(context.Background(), TabletReportDelta{
		TSUUID:        "ts-1",
		IsIncremental: false,
		SequenceNo:    2,
		Updated:       []string{"tablet-a"},
	}); err != nil {
		t.Fatalf("seed report: %v", err)
	}
	if err := sink.ApplyTabletReport(context.Background(), TabletReportDelta{
		TSUUID:        "ts-1",
		IsIncremental: true,
		SequenceNo:    3,
		RemovedIDs:    []string{"tablet-a"},
	}); err != nil {
		t.Fatalf("remove report: %v", err)
	}

	v, ok := sink.GetTablet("tablet-a")
	if !ok {
		t.Fatalf("expected tablet view")
	}
	if !v.Tombstoned {
		t.Fatalf("expected tablet tombstoned when no replicas remain")
	}
	if len(v.Replicas) != 0 {
		t.Fatalf("expected no replicas, got %d", len(v.Replicas))
	}
	if v.LastUpdated != 3 {
		t.Fatalf("unexpected last updated seq: %d", v.LastUpdated)
	}
}
