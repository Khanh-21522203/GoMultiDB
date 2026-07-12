package workload

import (
	"context"

	errors "GoMultiDB/v2/contracts/errors"
)

// WorkloadTarget is the interface a workload operates against.
// Implementations should issue actual reads/writes against the system
// under test.
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
	// Concurrency is the number of parallel workers. Defaults to
	// min(RPS, 64).
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

// RunWorkload runs the specified workload against the given target and
// returns the aggregated result. It respects ctx cancellation.
//
// Not yet implemented in this scaffold.
func RunWorkload(ctx context.Context, spec WorkloadSpec, target WorkloadTarget) (WorkloadResult, error) {
	return WorkloadResult{}, errors.ErrNotImplemented
}
