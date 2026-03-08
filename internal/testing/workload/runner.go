// Package workload provides a configurable workload driver for load and correctness testing.
package workload

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// WorkloadTarget is the interface a workload operates against. Implementations
// should issue actual reads/writes against the system under test.
type WorkloadTarget interface {
	// Read executes a read operation. Returns an error on failure.
	Read(ctx context.Context, tableID string) error
	// Write executes a write operation. Returns an error on failure.
	Write(ctx context.Context, tableID string) error
}

// WorkloadSpec describes a workload run.
type WorkloadSpec struct {
	// RPS is the target requests per second across all workers.
	RPS int
	// DurationSeconds is how long the workload runs.
	DurationSeconds int
	// ReadFraction in [0.0, 1.0] is the fraction of ops that are reads.
	ReadFraction float64
	// TableID identifies the target table.
	TableID string
	// Concurrency is the number of parallel workers. Defaults to min(RPS, 64).
	Concurrency int
}

// WorkloadResult captures the statistics of a completed workload run.
type WorkloadResult struct {
	TotalOps      int64
	Errors        int64
	P50LatencyMs  float64
	P95LatencyMs  float64
	P99LatencyMs  float64
	ThroughputOPS float64
	// DurationMs is the actual run duration.
	DurationMs float64
}

// RunWorkload runs the specified workload against the given target and returns
// the aggregated result. It respects ctx cancellation.
func RunWorkload(ctx context.Context, spec WorkloadSpec, target WorkloadTarget) (WorkloadResult, error) {
	if spec.RPS <= 0 {
		spec.RPS = 100
	}
	if spec.DurationSeconds <= 0 {
		spec.DurationSeconds = 1
	}
	if spec.Concurrency <= 0 {
		if spec.RPS < 64 {
			spec.Concurrency = spec.RPS
		} else {
			spec.Concurrency = 64
		}
		if spec.Concurrency < 1 {
			spec.Concurrency = 1
		}
	}

	duration := time.Duration(spec.DurationSeconds) * time.Second
	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	// Throttle: issue at most RPS tokens per second.
	interval := time.Duration(int64(time.Second) / int64(spec.RPS))

	var (
		totalOps atomic.Int64
		errCount atomic.Int64
		mu       sync.Mutex
		latencies []int64 // in microseconds
	)

	recordLatency := func(d time.Duration) {
		mu.Lock()
		latencies = append(latencies, d.Microseconds())
		mu.Unlock()
	}

	sem := make(chan struct{}, spec.Concurrency)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()

	for {
		select {
		case <-runCtx.Done():
			goto done
		case <-ticker.C:
		}

		sem <- struct{}{}
		opStart := time.Now()
		isRead := (totalOps.Load()%100) < int64(spec.ReadFraction*100)

		go func(isRead bool, opStart time.Time) {
			defer func() { <-sem }()
			var err error
			if isRead {
				err = target.Read(runCtx, spec.TableID)
			} else {
				err = target.Write(runCtx, spec.TableID)
			}
			totalOps.Add(1)
			if err != nil {
				errCount.Add(1)
			}
			recordLatency(time.Since(opStart))
		}(isRead, opStart)
	}

done:
	// Drain in-flight workers.
	for i := 0; i < spec.Concurrency; i++ {
		sem <- struct{}{}
	}

	actual := time.Since(start)
	result := WorkloadResult{
		TotalOps:   totalOps.Load(),
		Errors:     errCount.Load(),
		DurationMs: float64(actual.Milliseconds()),
	}
	if actual.Seconds() > 0 {
		result.ThroughputOPS = float64(result.TotalOps) / actual.Seconds()
	}

	mu.Lock()
	lats := latencies
	mu.Unlock()

	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		result.P50LatencyMs = float64(lats[percentileIndex(len(lats), 50)]) / 1000.0
		result.P95LatencyMs = float64(lats[percentileIndex(len(lats), 95)]) / 1000.0
		result.P99LatencyMs = float64(lats[percentileIndex(len(lats), 99)]) / 1000.0
	}

	return result, nil
}

func percentileIndex(n, pct int) int {
	idx := (n * pct / 100)
	if idx >= n {
		idx = n - 1
	}
	return idx
}
