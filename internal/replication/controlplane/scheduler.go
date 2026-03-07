package controlplane

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/replication/cdc"
	"GoMultiDB/internal/replication/xcluster"
)

type SchedulerConfig struct {
	PerJobInFlightCap int
	PollBatchSize     int
	FailureEveryTicks int
	InjectedDelay     time.Duration
}

type Scheduler struct {
	mu sync.Mutex

	cfg      SchedulerConfig
	registry *Registry
	cdcStore *cdc.Store
	loop     *xcluster.Loop

	inflight map[string]int
	ticks    int
}

func NewScheduler(cfg SchedulerConfig, registry *Registry, cdcStore *cdc.Store, loop *xcluster.Loop) (*Scheduler, error) {
	if registry == nil || cdcStore == nil || loop == nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "registry, cdc store, and loop are required", false, nil)
	}
	if cfg.PerJobInFlightCap <= 0 {
		cfg.PerJobInFlightCap = 64
	}
	if cfg.PollBatchSize <= 0 {
		cfg.PollBatchSize = 64
	}
	return &Scheduler{cfg: cfg, registry: registry, cdcStore: cdcStore, loop: loop, inflight: make(map[string]int)}, nil
}

func (s *Scheduler) Tick(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := s.maybeInjectTransportFault(ctx); err != nil {
		return err
	}

	jobs, err := s.registry.ListJobs(ctx)
	if err != nil {
		return err
	}
	streams, err := s.registry.ListStreams(ctx)
	if err != nil {
		return err
	}

	streamByID := make(map[string]Stream, len(streams))
	for _, st := range streams {
		streamByID[st.ID] = st
	}

	for _, job := range jobs {
		if job.State != JobStateRunning {
			continue
		}
		stream, ok := streamByID[job.StreamID]
		if !ok || stream.State != StreamStateRunning {
			continue
		}

		inflight := s.getInFlight(job.ID)
		if inflight >= s.cfg.PerJobInFlightCap {
			continue
		}
		allowance := s.cfg.PerJobInFlightCap - inflight
		batch := s.cfg.PollBatchSize
		if allowance < batch {
			batch = allowance
		}
		if batch <= 0 {
			continue
		}

		cp, err := s.cdcStore.GetCheckpoint(ctx, stream.ID, stream.TabletID)
		if err != nil {
			return err
		}
		poll, err := s.cdcStore.Poll(ctx, cdc.PollRequest{StreamID: stream.ID, TabletID: stream.TabletID, AfterSeq: cp.Sequence, MaxRecords: batch})
		if err != nil {
			return err
		}
		if len(poll.Events) == 0 {
			continue
		}

		s.addInFlight(job.ID, len(poll.Events))
		err = s.loop.ApplyBatch(ctx, poll.Events)
		s.addInFlight(job.ID, -len(poll.Events))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) maybeInjectTransportFault(ctx context.Context) error {
	s.mu.Lock()
	s.ticks++
	tick := s.ticks
	delay := s.cfg.InjectedDelay
	every := s.cfg.FailureEveryTicks
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	if every > 0 && tick%every == 0 {
		return dberrors.New(dberrors.ErrRetryableUnavailable, "injected transport fault", true, nil)
	}
	return nil
}

func (s *Scheduler) InFlight(ctx context.Context, jobID string) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if jobID == "" {
		return 0, dberrors.New(dberrors.ErrInvalidArgument, "job id is required", false, nil)
	}
	return s.getInFlight(jobID), nil
}

func (s *Scheduler) getInFlight(jobID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight[jobID]
}

func (s *Scheduler) addInFlight(jobID string, delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.inflight[jobID] + delta
	if next <= 0 {
		delete(s.inflight, jobID)
		return
	}
	s.inflight[jobID] = next
}
