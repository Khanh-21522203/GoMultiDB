package controlplane

import (
	"context"
	"testing"
	"time"

	"GoMultiDB/internal/replication/cdc"
	"GoMultiDB/internal/replication/xcluster"
)

type countingApplier struct {
	calls int
}

func (a *countingApplier) Apply(_ context.Context, _ cdc.Event) error {
	a.calls++
	return nil
}

func TestSchedulerProgressesRunningJob(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.CreateJob(ctx, "j1", "s1", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}

	store := cdc.NewStore()
	for i := 1; i <= 3; i++ {
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	applier := &countingApplier{}
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	sch, err := NewScheduler(SchedulerConfig{PerJobInFlightCap: 10, PollBatchSize: 10}, r, store, loop)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("tick: %v", err)
	}
	cp, err := store.GetCheckpoint(ctx, "s1", "t1")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 3 {
		t.Fatalf("expected checkpoint 3, got %d", cp.Sequence)
	}
	if applier.calls != 3 {
		t.Fatalf("expected 3 apply calls, got %d", applier.calls)
	}
}

func TestSchedulerSkipsPausedJob(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.CreateJob(ctx, "j1", "s1", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := r.PauseJob(ctx, "j1"); err != nil {
		t.Fatalf("pause job: %v", err)
	}

	store := cdc.NewStore()
	if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("append: %v", err)
	}

	applier := &countingApplier{}
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	sch, err := NewScheduler(SchedulerConfig{PerJobInFlightCap: 10, PollBatchSize: 10}, r, store, loop)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("tick paused: %v", err)
	}
	if applier.calls != 0 {
		t.Fatalf("expected no apply calls for paused job, got %d", applier.calls)
	}
}

func TestSchedulerInFlightCapLimitsPerTickBatch(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.CreateJob(ctx, "j1", "s1", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}

	store := cdc.NewStore()
	for i := 1; i <= 5; i++ {
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	applier := &countingApplier{}
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	sch, err := NewScheduler(SchedulerConfig{PerJobInFlightCap: 2, PollBatchSize: 10}, r, store, loop)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("tick 1: %v", err)
	}
	cp, err := store.GetCheckpoint(ctx, "s1", "t1")
	if err != nil {
		t.Fatalf("get checkpoint after tick1: %v", err)
	}
	if cp.Sequence != 2 {
		t.Fatalf("expected checkpoint 2 after capped tick, got %d", cp.Sequence)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	cp, err = store.GetCheckpoint(ctx, "s1", "t1")
	if err != nil {
		t.Fatalf("get checkpoint after tick2: %v", err)
	}
	if cp.Sequence != 4 {
		t.Fatalf("expected checkpoint 4 after second capped tick, got %d", cp.Sequence)
	}
}

func TestSchedulerInjectedTransportFaultDeterministic(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.CreateJob(ctx, "j1", "s1", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}

	store := cdc.NewStore()
	if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("append: %v", err)
	}

	loop, err := xcluster.NewLoop(xcluster.Config{}, store, &countingApplier{})
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	sch, err := NewScheduler(SchedulerConfig{PerJobInFlightCap: 10, PollBatchSize: 10, FailureEveryTicks: 2}, r, store, loop)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("tick 1 should pass: %v", err)
	}
	if err := sch.Tick(ctx); err == nil {
		t.Fatalf("tick 2 should fail due to injected deterministic fault")
	}
}

func TestSchedulerOwnershipEpochChangeClampsFirstBatch(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	if err := r.CreateStream(ctx, "s-own", "t-own"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.UpdatePrimaryOwnership(ctx, "s-own", "ts-1", 1, true); err != nil {
		t.Fatalf("update ownership: %v", err)
	}
	if err := r.CreateJob(ctx, "j-own", "s-own", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}

	store := cdc.NewStore()
	for i := 1; i <= 3; i++ {
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s-own", TabletID: "t-own", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	applier := &countingApplier{}
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	sch, err := NewScheduler(SchedulerConfig{PerJobInFlightCap: 10, PollBatchSize: 10}, r, store, loop)
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("first ownership tick: %v", err)
	}
	cp, err := store.GetCheckpoint(ctx, "s-own", "t-own")
	if err != nil {
		t.Fatalf("checkpoint after first tick: %v", err)
	}
	if cp.Sequence != 1 {
		t.Fatalf("expected first ownership tick to process one event, got checkpoint %d", cp.Sequence)
	}

	if err := sch.Tick(ctx); err != nil {
		t.Fatalf("second ownership tick: %v", err)
	}
	cp, err = store.GetCheckpoint(ctx, "s-own", "t-own")
	if err != nil {
		t.Fatalf("checkpoint after second tick: %v", err)
	}
	if cp.Sequence != 3 {
		t.Fatalf("expected second tick to process remaining events, got checkpoint %d", cp.Sequence)
	}
}
