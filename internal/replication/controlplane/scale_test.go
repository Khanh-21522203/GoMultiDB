package controlplane

import (
	"context"
	"fmt"
	"testing"

	"GoMultiDB/internal/replication/cdc"
)

func TestScaleDeterministicRegistryStress(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	store := cdc.NewStore()

	const streams = 40
	const jobs = 40

	for i := 0; i < streams; i++ {
		sid := fmt.Sprintf("s-%03d", i)
		tid := fmt.Sprintf("t-%03d", i)
		if err := r.CreateStream(ctx, sid, tid); err != nil {
			t.Fatalf("create stream %s: %v", sid, err)
		}
		for seq := 1; seq <= 10; seq++ {
			if err := store.AppendEvent(ctx, cdc.Event{StreamID: sid, TabletID: tid, Sequence: uint64(seq)}); err != nil {
				t.Fatalf("append event sid=%s seq=%d: %v", sid, seq, err)
			}
		}
		if err := store.AdvanceCheckpoint(ctx, cdc.Checkpoint{StreamID: sid, TabletID: tid, Sequence: 7}); err != nil {
			t.Fatalf("advance checkpoint sid=%s: %v", sid, err)
		}
	}

	for i := 0; i < jobs; i++ {
		jid := fmt.Sprintf("j-%03d", i)
		sid := fmt.Sprintf("s-%03d", i%streams)
		if err := r.CreateJob(ctx, jid, sid, "cluster-b"); err != nil {
			t.Fatalf("create job %s: %v", jid, err)
		}
	}

	snap, err := r.Snapshot(ctx, store, nil)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap.Streams) != streams {
		t.Fatalf("expected %d streams, got %d", streams, len(snap.Streams))
	}
	if len(snap.Jobs) != jobs {
		t.Fatalf("expected %d jobs, got %d", jobs, len(snap.Jobs))
	}

	for _, s := range snap.Streams {
		if s.Checkpoint != 7 {
			t.Fatalf("expected checkpoint 7 for %s, got %d", s.ID, s.Checkpoint)
		}
		if s.LagEvents != 3 {
			t.Fatalf("expected lag 3 for %s, got %d", s.ID, s.LagEvents)
		}
	}
}
