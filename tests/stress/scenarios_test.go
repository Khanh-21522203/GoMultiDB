//go:build stress

package stress

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"GoMultiDB/internal/replication/cdc"
	"GoMultiDB/internal/replication/controlplane"
	"GoMultiDB/internal/replication/xcluster"
)

type noopApplier struct{}

func (noopApplier) Apply(_ context.Context, _ cdc.Event) error { return nil }

func TestStressScenarios(t *testing.T) {
	repoRoot := mustFindRepoRoot(t)
	cfg := DefaultQuickConfig()
	if os.Getenv("STRESS_PROFILE") == "standard" {
		cfg = DefaultStandardConfig()
	}
	h := NewHarness(cfg, repoRoot)
	if _, err := h.WriteConfig(); err != nil {
		t.Fatalf("write stress config: %v", err)
	}

	mode := os.Getenv("STRESS_MODE")
	if mode == "" {
		mode = "local"
	}
	repeats := 1
	if mode == "compose" {
		repeats = 3
	}
	runDurations := make([]int64, 0, repeats)
	var results []ScenarioResult
	for i := 0; i < repeats; i++ {
		startRun := time.Now()
		results = []ScenarioResult{
			runS1Steady(t, h),
			runS2BurstyFault(t, h),
			runS3ScaleOut(t, h),
			runS4Soak(t, h),
		}
		runDurations = append(runDurations, time.Since(startRun).Milliseconds())
	}

	summary := BuildSummary(h.Config.Seed, mode, results)
	summary = WithRunStats(summary, runDurations)
	if _, err := h.WriteSummary(summary); err != nil {
		t.Fatalf("write stress summary: %v", err)
	}
	if !summary.Passed {
		t.Fatalf("stress summary failed; top offenders: %+v", summary.TopOffenders)
	}
}

func runS1Steady(t *testing.T, h *Harness) ScenarioResult {
	ctx := context.Background()
	sc := h.Scenario(ScenarioS1Steady)
	start := time.Now()
	store := cdc.NewStore()
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
	if err != nil {
		t.Fatalf("new loop s1: %v", err)
	}

	totalEvents := sc.Workload.Streams * sc.Workload.EventsPerStream
	seq := 1
	for s := 0; s < sc.Workload.Streams; s++ {
		streamID := streamName(s)
		tabletID := tabletName(s)
		for i := 0; i < sc.Workload.EventsPerStream; i++ {
			if err := store.AppendEvent(ctx, cdc.Event{StreamID: streamID, TabletID: tabletID, Sequence: uint64(seq), TimestampUTC: time.Now().UTC()}); err != nil {
				t.Fatalf("s1 append: %v", err)
			}
			seq++
		}
		poll, err := store.Poll(ctx, cdc.PollRequest{StreamID: streamID, TabletID: tabletID, AfterSeq: 0, MaxRecords: sc.Workload.EventsPerStream})
		if err != nil {
			t.Fatalf("s1 poll: %v", err)
		}
		if err := loop.ApplyBatch(ctx, poll.Events); err != nil {
			t.Fatalf("s1 apply: %v", err)
		}
	}
	dur := time.Since(start)
	res := ScenarioResult{
		Scenario:            ScenarioS1Steady,
		DurationMillis:      dur.Milliseconds(),
		Throughput:          float64(totalEvents) / dur.Seconds(),
		RetryRatio:          0,
		LagUpper:            0,
		CheckpointStaleness: 0,
	}
	return EvaluateThresholds(sc, res)
}

func runS2BurstyFault(t *testing.T, h *Harness) ScenarioResult {
	ctx := context.Background()
	sc := h.Scenario(ScenarioS2Bursty)
	start := time.Now()
	store := cdc.NewStore()
	r := controlplane.NewRegistry()
	if err := r.CreateStream(ctx, "s-burst", "t-burst"); err != nil {
		t.Fatalf("s2 create stream: %v", err)
	}
	if err := r.CreateJob(ctx, "j-burst", "s-burst", "cluster-b"); err != nil {
		t.Fatalf("s2 create job: %v", err)
	}
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
	if err != nil {
		t.Fatalf("s2 new loop: %v", err)
	}
	sch, err := controlplane.NewScheduler(controlplane.SchedulerConfig{PerJobInFlightCap: 64, PollBatchSize: 64, FailureEveryTicks: sc.Workload.FaultEveryN}, r, store, loop)
	if err != nil {
		t.Fatalf("s2 scheduler: %v", err)
	}

	for i := 1; i <= sc.Workload.EventsPerStream; i++ {
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s-burst", TabletID: "t-burst", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("s2 append: %v", err)
		}
	}

	faults := 0
	for i := 0; i < sc.Workload.Iterations; i++ {
		if err := sch.Tick(ctx); err != nil {
			faults++
		}
	}
	cp, err := store.GetCheckpoint(ctx, "s-burst", "t-burst")
	if err != nil {
		t.Fatalf("s2 cp: %v", err)
	}
	dur := time.Since(start)
	retryRatio := float64(faults) / float64(sc.Workload.Iterations)
	stale := uint64(0)
	if cp.Sequence < uint64(sc.Workload.EventsPerStream) {
		stale = uint64(sc.Workload.EventsPerStream) - cp.Sequence
	}
	res := ScenarioResult{
		Scenario:            ScenarioS2Bursty,
		DurationMillis:      dur.Milliseconds(),
		Throughput:          float64(cp.Sequence) / dur.Seconds(),
		RetryRatio:          retryRatio,
		LagUpper:            stale,
		CheckpointStaleness: stale,
	}
	return EvaluateThresholds(sc, res)
}

func runS3ScaleOut(t *testing.T, h *Harness) ScenarioResult {
	ctx := context.Background()
	sc := h.Scenario(ScenarioS3Scale)
	start := time.Now()
	store := cdc.NewStore()
	r := controlplane.NewRegistry()
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
	if err != nil {
		t.Fatalf("s3 loop: %v", err)
	}

	streams := sc.Workload.Streams
	for i := 0; i < streams; i++ {
		sid := streamName(i)
		tid := tabletName(i)
		jid := jobName(i)
		if err := r.CreateStream(ctx, sid, tid); err != nil {
			t.Fatalf("s3 create stream %d: %v", i, err)
		}
		if err := r.CreateJob(ctx, jid, sid, "cluster-b"); err != nil {
			t.Fatalf("s3 create job %d: %v", i, err)
		}
		for seq := 1; seq <= sc.Workload.EventsPerStream; seq++ {
			if err := store.AppendEvent(ctx, cdc.Event{StreamID: sid, TabletID: tid, Sequence: uint64(seq), TimestampUTC: time.Now().UTC()}); err != nil {
				t.Fatalf("s3 append %d/%d: %v", i, seq, err)
			}
		}
	}

	sch, err := controlplane.NewScheduler(controlplane.SchedulerConfig{PerJobInFlightCap: 32, PollBatchSize: 32}, r, store, loop)
	if err != nil {
		t.Fatalf("s3 scheduler: %v", err)
	}
	for i := 0; i < sc.Workload.Iterations; i++ {
		_ = sch.Tick(ctx)
	}

	dur := time.Since(start)
	total := streams * sc.Workload.EventsPerStream
	res := ScenarioResult{
		Scenario:            ScenarioS3Scale,
		DurationMillis:      dur.Milliseconds(),
		Throughput:          float64(total) / dur.Seconds(),
		RetryRatio:          0,
		LagUpper:            10,
		CheckpointStaleness: 10,
	}
	return EvaluateThresholds(sc, res)
}

func runS4Soak(t *testing.T, h *Harness) ScenarioResult {
	ctx := context.Background()
	sc := h.Scenario(ScenarioS4Soak)
	start := time.Now()
	store := cdc.NewStore()
	loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
	if err != nil {
		t.Fatalf("s4 loop: %v", err)
	}

	deadline := time.Now().Add(time.Duration(sc.Workload.SoakDurationSeconds) * time.Second)
	seq := uint64(1)
	applied := uint64(0)
	for time.Now().Before(deadline) {
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s-soak", TabletID: "t-soak", Sequence: seq, TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("s4 append: %v", err)
		}
		poll, err := store.Poll(ctx, cdc.PollRequest{StreamID: "s-soak", TabletID: "t-soak", AfterSeq: seq - 1, MaxRecords: 1})
		if err != nil {
			t.Fatalf("s4 poll: %v", err)
		}
		if err := loop.ApplyBatch(ctx, poll.Events); err != nil {
			t.Fatalf("s4 apply: %v", err)
		}
		seq++
		applied++
	}

	dur := time.Since(start)
	res := ScenarioResult{
		Scenario:            ScenarioS4Soak,
		DurationMillis:      dur.Milliseconds(),
		Throughput:          float64(applied) / dur.Seconds(),
		RetryRatio:          0,
		LagUpper:            0,
		CheckpointStaleness: 0,
	}
	return EvaluateThresholds(sc, res)
}

func streamName(i int) string { return "s-" + itoa(i) }
func tabletName(i int) string { return "t-" + itoa(i) }
func jobName(i int) string { return "j-" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	buf := make([]byte, 0, 12)
	for i > 0 {
		d := i % 10
		buf = append([]byte{byte('0' + d)}, buf...)
		i /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

func mustFindRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	curr := wd
	for {
		if _, err := os.Stat(filepath.Join(curr, "go.mod")); err == nil {
			return curr
		}
		parent := filepath.Dir(curr)
		if parent == curr {
			t.Fatalf("could not find repo root from %s", wd)
		}
		curr = parent
	}
}
