package xcluster

import (
	"context"
	"errors"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/replication/cdc"
)

type stableApplier struct{}

func (stableApplier) Apply(_ context.Context, _ cdc.Event) error { return nil }

type alwaysFailApplier struct{}

func (alwaysFailApplier) Apply(_ context.Context, _ cdc.Event) error { return errors.New("forced failure") }

func TestContractStabilityCanonicalErrors(t *testing.T) {
	store := cdc.NewStore()
	loop, err := NewLoop(Config{}, store, stableApplier{})
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()}); err == nil {
		t.Fatalf("expected invalid argument for missing stream id")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}

	failingLoop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}}, store, alwaysFailApplier{})
	if err != nil {
		t.Fatalf("new failing loop: %v", err)
	}
	if err := failingLoop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()}); err == nil {
		t.Fatalf("expected retryable unavailable after retries exhausted")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable code, got %s", n.Code)
	}
}

func TestReplayDeterminismForLoopState(t *testing.T) {
	sequence := []cdc.Event{
		{StreamID: "sdet", TabletID: "tdet", Sequence: 1, TimestampUTC: time.Now().UTC()},
		{StreamID: "sdet", TabletID: "tdet", Sequence: 2, TimestampUTC: time.Now().UTC()},
		{StreamID: "sdet", TabletID: "tdet", Sequence: 2, TimestampUTC: time.Now().UTC()},
		{StreamID: "sdet", TabletID: "tdet", Sequence: 3, TimestampUTC: time.Now().UTC()},
	}

	run := func() (Stats, cdc.Checkpoint, cdc.LagSnapshot, error) {
		store := cdc.NewStore()
		loop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}}, store, stableApplier{})
		if err != nil {
			return Stats{}, cdc.Checkpoint{}, cdc.LagSnapshot{}, err
		}
		for _, ev := range sequence {
			if err := loop.ApplyEvent(context.Background(), ev); err != nil {
				return Stats{}, cdc.Checkpoint{}, cdc.LagSnapshot{}, err
			}
		}
		stats, err := loop.Stats(context.Background())
		if err != nil {
			return Stats{}, cdc.Checkpoint{}, cdc.LagSnapshot{}, err
		}
		cp, err := store.GetCheckpoint(context.Background(), "sdet", "tdet")
		if err != nil {
			return Stats{}, cdc.Checkpoint{}, cdc.LagSnapshot{}, err
		}
		lag, err := loop.LagSnapshot(context.Background(), "sdet", "tdet")
		if err != nil {
			return Stats{}, cdc.Checkpoint{}, cdc.LagSnapshot{}, err
		}
		return stats, cp, lag, nil
	}

	s1, cp1, lag1, err := run()
	if err != nil {
		t.Fatalf("run1: %v", err)
	}
	s2, cp2, lag2, err := run()
	if err != nil {
		t.Fatalf("run2: %v", err)
	}

	if s1.AppliedEvents != s2.AppliedEvents || s1.DuplicateEvents != s2.DuplicateEvents {
		t.Fatalf("stats determinism mismatch")
	}
	if cp1.Sequence != cp2.Sequence {
		t.Fatalf("checkpoint determinism mismatch: %d vs %d", cp1.Sequence, cp2.Sequence)
	}
	if lag1.LagEvents != lag2.LagEvents {
		t.Fatalf("lag determinism mismatch: %d vs %d", lag1.LagEvents, lag2.LagEvents)
	}
}

func TestBoundaryGuardResumeLagCompatibility(t *testing.T) {
	store := cdc.NewStore()
	loop, err := NewLoop(Config{}, store, stableApplier{})
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "sbg", TabletID: "tbg", Sequence: 4, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	cp, err := loop.ResumeCheckpoint(context.Background(), "sbg", "tbg")
	if err != nil {
		t.Fatalf("resume checkpoint: %v", err)
	}
	lag, err := loop.LagSnapshot(context.Background(), "sbg", "tbg")
	if err != nil {
		t.Fatalf("lag snapshot: %v", err)
	}
	if cp.Sequence != lag.Checkpoint {
		t.Fatalf("boundary mismatch: resume seq %d != lag checkpoint %d", cp.Sequence, lag.Checkpoint)
	}
}
