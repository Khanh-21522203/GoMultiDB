package workload_test

import (
	"context"
	"sync/atomic"
	"testing"

	"GoMultiDB/internal/testing/workload"
)

// countingTarget counts reads and writes issued against it.
type countingTarget struct {
	reads  atomic.Int64
	writes atomic.Int64
}

func (c *countingTarget) Read(_ context.Context, _ string) error {
	c.reads.Add(1)
	return nil
}

func (c *countingTarget) Write(_ context.Context, _ string) error {
	c.writes.Add(1)
	return nil
}

func TestRunWorkloadCompletesWithinDuration(t *testing.T) {
	target := &countingTarget{}
	result, err := workload.RunWorkload(context.Background(), workload.WorkloadSpec{
		RPS:             20,
		DurationSeconds: 1,
		ReadFraction:    0.5,
		TableID:         "test-table",
	}, target)
	if err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	if result.TotalOps == 0 {
		t.Fatalf("expected > 0 total ops, got 0")
	}
}

func TestRunWorkloadRespectsDuration(t *testing.T) {
	target := &countingTarget{}
	result, err := workload.RunWorkload(context.Background(), workload.WorkloadSpec{
		RPS:             50,
		DurationSeconds: 1,
		ReadFraction:    1.0,
	}, target)
	if err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	// Duration should be within ~2s (1s run + scheduling overhead).
	if result.DurationMs > 3000 {
		t.Fatalf("expected run to complete in ~1s, took %.0f ms", result.DurationMs)
	}
}

func TestRunWorkloadReadFractionAllReads(t *testing.T) {
	target := &countingTarget{}
	_, err := workload.RunWorkload(context.Background(), workload.WorkloadSpec{
		RPS:             10,
		DurationSeconds: 1,
		ReadFraction:    1.0,
		TableID:         "tbl",
	}, target)
	if err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	// With ReadFraction=1.0, writes should be 0.
	if target.writes.Load() != 0 {
		t.Fatalf("expected 0 writes with ReadFraction=1.0, got %d", target.writes.Load())
	}
	if target.reads.Load() == 0 {
		t.Fatalf("expected reads > 0")
	}
}

func TestRunWorkloadThroughputNonZero(t *testing.T) {
	target := &countingTarget{}
	result, err := workload.RunWorkload(context.Background(), workload.WorkloadSpec{
		RPS:             30,
		DurationSeconds: 1,
	}, target)
	if err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	if result.ThroughputOPS <= 0 {
		t.Fatalf("expected non-zero throughput, got %v", result.ThroughputOPS)
	}
}

func TestRunWorkloadLatencyPercentiles(t *testing.T) {
	target := &countingTarget{}
	result, err := workload.RunWorkload(context.Background(), workload.WorkloadSpec{
		RPS:             20,
		DurationSeconds: 1,
	}, target)
	if err != nil {
		t.Fatalf("RunWorkload: %v", err)
	}
	if result.TotalOps > 0 {
		if result.P50LatencyMs < 0 {
			t.Fatalf("P50 should be >= 0, got %v", result.P50LatencyMs)
		}
		if result.P95LatencyMs < result.P50LatencyMs {
			t.Fatalf("P95 should be >= P50: P50=%v P95=%v", result.P50LatencyMs, result.P95LatencyMs)
		}
		if result.P99LatencyMs < result.P95LatencyMs {
			t.Fatalf("P99 should be >= P95: P95=%v P99=%v", result.P95LatencyMs, result.P99LatencyMs)
		}
	}
}

func TestRunWorkloadContextCancellation(t *testing.T) {
	target := &countingTarget{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancelled

	result, err := workload.RunWorkload(ctx, workload.WorkloadSpec{
		RPS:             100,
		DurationSeconds: 60,
	}, target)
	if err != nil {
		t.Fatalf("RunWorkload with cancelled ctx: %v", err)
	}
	// With instant cancel, should complete very quickly.
	if result.DurationMs > 2000 {
		t.Fatalf("expected fast exit on cancel, took %.0f ms", result.DurationMs)
	}
}
