package controlplane

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/replication/cdc"
	"GoMultiDB/v2/replication/xcluster"
)

// SchedulerConfig configures a Scheduler's batching and fault-injection
// behavior.
type SchedulerConfig struct {
	PerJobInFlightCap int
	PollBatchSize     int
	FailureEveryTicks int
	InjectedDelay     time.Duration
}

// Scheduler periodically polls each running stream's CDC store and
// drives its xcluster.Loop to apply batches, honoring a per-job in-flight
// cap and reducing batch size for one tick after an ownership-epoch
// change.
//
// Scaffold stub: all methods are unimplemented.
type Scheduler struct {
	mu sync.Mutex

	cfg      SchedulerConfig
	registry *Registry
	cdcStore *cdc.Store
	loop     *xcluster.Loop

	inflight      map[string]int
	seenOwnership map[string]uint64
	ticks         int
}

// NewScheduler returns a Scheduler driving registry's streams and jobs,
// polling cdcStore and applying through loop.
func NewScheduler(cfg SchedulerConfig, registry *Registry, cdcStore *cdc.Store, loop *xcluster.Loop) (*Scheduler, error) {
	return &Scheduler{}, nil
}

// Tick performs a single scheduling pass: for each running job whose
// stream is running, it polls up to the job's remaining in-flight
// allowance and applies the resulting batch through the Loop.
//
// Not yet implemented in this scaffold.
func (s *Scheduler) Tick(ctx context.Context) error {
	return dberrors.ErrNotImplemented
}

// InFlight returns the number of events currently in flight for jobID.
//
// Not yet implemented in this scaffold.
func (s *Scheduler) InFlight(ctx context.Context, jobID string) (int, error) {
	return 0, dberrors.ErrNotImplemented
}
