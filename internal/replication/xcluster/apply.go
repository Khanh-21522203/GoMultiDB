package xcluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/replication/cdc"
)

type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

type Config struct {
	Retry RetryPolicy
}

type Stats struct {
	AppliedEvents   uint64
	DuplicateEvents uint64
	RetryCount      uint64
	FailureCount    uint64
	LastAppliedSeq  uint64
	LastUpdated     time.Time
}

type Status struct {
	Config Config
	Stats  Stats
}

type Applier interface {
	Apply(ctx context.Context, ev cdc.Event) error
}

type CheckpointStore interface {
	AdvanceCheckpoint(ctx context.Context, cp cdc.Checkpoint) error
	GetCheckpoint(ctx context.Context, streamID, tabletID string) (cdc.Checkpoint, error)
}

type LagProvider interface {
	LagSnapshot(ctx context.Context, streamID, tabletID string) (cdc.LagSnapshot, error)
}

type Loop struct {
	mu sync.RWMutex

	cfg         Config
	store       CheckpointStore
	lagProvider LagProvider
	applier     Applier
	appliedSet  map[string]struct{}
	stats       Stats
}

func NewLoop(cfg Config, store CheckpointStore, applier Applier) (*Loop, error) {
	if store == nil {
		return nil, fmt.Errorf("checkpoint store is required")
	}
	if applier == nil {
		return nil, fmt.Errorf("applier is required")
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 3
	}
	if cfg.Retry.Backoff <= 0 {
		cfg.Retry.Backoff = 10 * time.Millisecond
	}
	loop := &Loop{
		cfg:         cfg,
		store:       store,
		applier:     applier,
		appliedSet:  make(map[string]struct{}),
		stats:       Stats{LastUpdated: time.Now().UTC()},
		lagProvider: nil,
	}
	if lp, ok := store.(LagProvider); ok {
		loop.lagProvider = lp
	}
	return loop, nil
}

func (l *Loop) ApplyBatch(ctx context.Context, events []cdc.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for _, ev := range events {
		if err := l.ApplyEvent(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

func (l *Loop) ApplyEvent(ctx context.Context, ev cdc.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if ev.StreamID == "" || ev.TabletID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}
	if ev.Sequence == 0 {
		return dberrors.New(dberrors.ErrInvalidArgument, "sequence must be >= 1", false, nil)
	}

	cp, err := l.store.GetCheckpoint(ctx, ev.StreamID, ev.TabletID)
	if err != nil {
		return err
	}
	if ev.Sequence <= cp.Sequence {
		l.mu.Lock()
		l.stats.DuplicateEvents++
		l.stats.LastUpdated = time.Now().UTC()
		l.mu.Unlock()
		log.Printf("xcluster duplicate event stream=%s tablet=%s seq=%d", ev.StreamID, ev.TabletID, ev.Sequence)
		return nil
	}

	key := dedupeKey(ev)
	l.mu.RLock()
	_, seen := l.appliedSet[key]
	l.mu.RUnlock()
	if seen {
		l.mu.Lock()
		l.stats.DuplicateEvents++
		l.stats.LastUpdated = time.Now().UTC()
		l.mu.Unlock()
		log.Printf("xcluster duplicate dedupe event stream=%s tablet=%s seq=%d", ev.StreamID, ev.TabletID, ev.Sequence)
		return nil
	}

	if err := l.applyWithRetry(ctx, ev); err != nil {
		l.mu.Lock()
		l.stats.FailureCount++
		l.stats.LastUpdated = time.Now().UTC()
		l.mu.Unlock()
		log.Printf("xcluster apply failed stream=%s tablet=%s seq=%d err=%v", ev.StreamID, ev.TabletID, ev.Sequence, err)
		return err
	}

	if err := l.store.AdvanceCheckpoint(ctx, cdc.Checkpoint{StreamID: ev.StreamID, TabletID: ev.TabletID, Sequence: ev.Sequence}); err != nil {
		return err
	}

	l.mu.Lock()
	l.appliedSet[key] = struct{}{}
	l.stats.AppliedEvents++
	l.stats.LastAppliedSeq = ev.Sequence
	l.stats.LastUpdated = time.Now().UTC()
	l.mu.Unlock()
	log.Printf("xcluster apply success stream=%s tablet=%s seq=%d", ev.StreamID, ev.TabletID, ev.Sequence)
	return nil
}

func (l *Loop) ResumeCheckpoint(ctx context.Context, streamID, tabletID string) (cdc.Checkpoint, error) {
	select {
	case <-ctx.Done():
		return cdc.Checkpoint{}, ctx.Err()
	default:
	}
	if streamID == "" || tabletID == "" {
		return cdc.Checkpoint{}, dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}
	cp, err := l.store.GetCheckpoint(ctx, streamID, tabletID)
	if err != nil {
		return cdc.Checkpoint{}, err
	}
	log.Printf("xcluster resume checkpoint stream=%s tablet=%s seq=%d", streamID, tabletID, cp.Sequence)
	return cp, nil
}

func (l *Loop) LagSnapshot(ctx context.Context, streamID, tabletID string) (cdc.LagSnapshot, error) {
	select {
	case <-ctx.Done():
		return cdc.LagSnapshot{}, ctx.Err()
	default:
	}
	if streamID == "" || tabletID == "" {
		return cdc.LagSnapshot{}, dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}
	if l.lagProvider != nil {
		return l.lagProvider.LagSnapshot(ctx, streamID, tabletID)
	}
	cp, err := l.store.GetCheckpoint(ctx, streamID, tabletID)
	if err != nil {
		return cdc.LagSnapshot{}, err
	}
	return cdc.LagSnapshot{StreamID: streamID, TabletID: tabletID, LatestSeq: cp.Sequence, Checkpoint: cp.Sequence, LagEvents: 0, CollectedAt: time.Now().UTC()}, nil
}

func (l *Loop) Stats(ctx context.Context) (Stats, error) {
	select {
	case <-ctx.Done():
		return Stats{}, ctx.Err()
	default:
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.stats, nil
}

func (l *Loop) Status(ctx context.Context) (Status, error) {
	select {
	case <-ctx.Done():
		return Status{}, ctx.Err()
	default:
	}
	stats, err := l.Stats(ctx)
	if err != nil {
		return Status{}, err
	}
	return Status{Config: l.cfg, Stats: stats}, nil
}

func (l *Loop) applyWithRetry(ctx context.Context, ev cdc.Event) error {
	var lastErr error
	for attempt := 1; attempt <= l.cfg.Retry.MaxAttempts; attempt++ {
		if err := l.applier.Apply(ctx, ev); err == nil {
			return nil
		} else {
			lastErr = err
			if attempt < l.cfg.Retry.MaxAttempts {
				l.mu.Lock()
				l.stats.RetryCount++
				l.stats.LastUpdated = time.Now().UTC()
				l.mu.Unlock()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(l.cfg.Retry.Backoff):
				}
			}
		}
	}
	return dberrors.New(dberrors.ErrRetryableUnavailable, "xcluster apply retries exhausted", true, lastErr)
}

func dedupeKey(ev cdc.Event) string {
	return fmt.Sprintf("%s|%s|%d", ev.StreamID, ev.TabletID, ev.Sequence)
}
