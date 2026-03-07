package controlplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/replication/cdc"
	"GoMultiDB/internal/replication/xcluster"
)

type flakyApplier struct {
	mu        sync.Mutex
	failFirst int
}

func (f *flakyApplier) Apply(_ context.Context, _ cdc.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFirst > 0 {
		f.failFirst--
		return errors.New("transient applier failure")
	}
	return nil
}

func TestRobustnessPauseResumeStopUnderTransientApplyFailures(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	store := cdc.NewStore()
	applier := &flakyApplier{failFirst: 2}
	loop, err := xcluster.NewLoop(xcluster.Config{Retry: xcluster.RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.CreateJob(ctx, "j1", "s1", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if err := r.PauseJob(ctx, "j1"); err != nil {
		t.Fatalf("pause job: %v", err)
	}
	if err := r.ResumeJob(ctx, "j1"); err != nil {
		t.Fatalf("resume job: %v", err)
	}

	if err := loop.ApplyEvent(ctx, cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply with retries should eventually succeed: %v", err)
	}

	if err := r.StopJob(ctx, "j1"); err != nil {
		t.Fatalf("stop job: %v", err)
	}
	if err := r.ResumeJob(ctx, "j1"); err == nil {
		t.Fatalf("expected conflict resuming stopped job")
	}
}

func TestRobustnessCheckpointMonotonicityMultiStreamInterleaving(t *testing.T) {
	ctx := context.Background()
	store := cdc.NewStore()
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, &flakyApplier{})
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	events := []cdc.Event{
		{StreamID: "sa", TabletID: "ta", Sequence: 1, TimestampUTC: time.Now().UTC()},
		{StreamID: "sb", TabletID: "tb", Sequence: 1, TimestampUTC: time.Now().UTC()},
		{StreamID: "sa", TabletID: "ta", Sequence: 2, TimestampUTC: time.Now().UTC()},
		{StreamID: "sb", TabletID: "tb", Sequence: 2, TimestampUTC: time.Now().UTC()},
		{StreamID: "sa", TabletID: "ta", Sequence: 2, TimestampUTC: time.Now().UTC()}, // duplicate
		{StreamID: "sb", TabletID: "tb", Sequence: 3, TimestampUTC: time.Now().UTC()},
	}
	for _, ev := range events {
		if err := loop.ApplyEvent(ctx, ev); err != nil {
			t.Fatalf("apply event %+v: %v", ev, err)
		}
	}

	cpA, err := store.GetCheckpoint(ctx, "sa", "ta")
	if err != nil {
		t.Fatalf("get cpA: %v", err)
	}
	cpB, err := store.GetCheckpoint(ctx, "sb", "tb")
	if err != nil {
		t.Fatalf("get cpB: %v", err)
	}
	if cpA.Sequence != 2 {
		t.Fatalf("expected cpA=2, got %d", cpA.Sequence)
	}
	if cpB.Sequence != 3 {
		t.Fatalf("expected cpB=3, got %d", cpB.Sequence)
	}
}

func TestRobustnessPartitionRejoinMatrix(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	store := cdc.NewStore()
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, &flakyApplier{})
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream s1: %v", err)
	}
	if err := r.CreateStream(ctx, "s2", "t2"); err != nil {
		t.Fatalf("create stream s2: %v", err)
	}
	if err := r.CreateJob(ctx, "j1", "s1", "cluster-b"); err != nil {
		t.Fatalf("create job j1: %v", err)
	}
	if err := r.CreateJob(ctx, "j2", "s2", "cluster-b"); err != nil {
		t.Fatalf("create job j2: %v", err)
	}

	for i := 1; i <= 3; i++ {
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append s1 %d: %v", i, err)
		}
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s2", TabletID: "t2", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append s2 %d: %v", i, err)
		}
	}

	sch, err := NewScheduler(SchedulerConfig{PerJobInFlightCap: 2, PollBatchSize: 2, FailureEveryTicks: 2}, r, store, loop)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("tick 1 should pass: %v", err)
	}
	if err := sch.Tick(ctx); err == nil {
		t.Fatalf("tick 2 should fail due to injected fault")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable on injected fault, got %s", n.Code)
	}
	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("tick 3 should pass after rejoin: %v", err)
	}

	cp1, err := store.GetCheckpoint(ctx, "s1", "t1")
	if err != nil {
		t.Fatalf("get cp1: %v", err)
	}
	cp2, err := store.GetCheckpoint(ctx, "s2", "t2")
	if err != nil {
		t.Fatalf("get cp2: %v", err)
	}
	if cp1.Sequence == 0 || cp2.Sequence == 0 {
		t.Fatalf("expected both streams to make forward progress after rejoin, got cp1=%d cp2=%d", cp1.Sequence, cp2.Sequence)
	}
}
