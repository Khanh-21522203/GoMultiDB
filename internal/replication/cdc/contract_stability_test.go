package cdc

import (
	"context"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestContractStabilityCanonicalErrors(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	if err := s.AppendEvent(ctx, Event{StreamID: "", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()}); err == nil {
		t.Fatalf("expected invalid argument for missing stream id")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}

	if err := s.AdvanceCheckpoint(ctx, Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 5}); err != nil {
		t.Fatalf("advance baseline: %v", err)
	}
	if err := s.AdvanceCheckpoint(ctx, Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 4}); err == nil {
		t.Fatalf("expected conflict for checkpoint regression")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}
}

func TestReplayDeterminismForStoreState(t *testing.T) {
	ctx := context.Background()
	sequence := []Event{
		{StreamID: "sdet", TabletID: "tdet", Sequence: 1, TimestampUTC: time.Now().UTC()},
		{StreamID: "sdet", TabletID: "tdet", Sequence: 2, TimestampUTC: time.Now().UTC()},
		{StreamID: "sdet", TabletID: "tdet", Sequence: 2, TimestampUTC: time.Now().UTC()}, // idempotent duplicate
		{StreamID: "sdet", TabletID: "tdet", Sequence: 3, TimestampUTC: time.Now().UTC()},
	}

	finalize := func() (Checkpoint, LagSnapshot, Status, error) {
		s := NewStore()
		for _, ev := range sequence {
			if err := s.AppendEvent(ctx, ev); err != nil {
				return Checkpoint{}, LagSnapshot{}, Status{}, err
			}
		}
		if err := s.AdvanceCheckpoint(ctx, Checkpoint{StreamID: "sdet", TabletID: "tdet", Sequence: 2}); err != nil {
			return Checkpoint{}, LagSnapshot{}, Status{}, err
		}
		cp, err := s.GetCheckpoint(ctx, "sdet", "tdet")
		if err != nil {
			return Checkpoint{}, LagSnapshot{}, Status{}, err
		}
		lag, err := s.LagSnapshot(ctx, "sdet", "tdet")
		if err != nil {
			return Checkpoint{}, LagSnapshot{}, Status{}, err
		}
		status, err := s.Status(ctx)
		if err != nil {
			return Checkpoint{}, LagSnapshot{}, Status{}, err
		}
		return cp, lag, status, nil
	}

	cp1, lag1, st1, err := finalize()
	if err != nil {
		t.Fatalf("first finalize: %v", err)
	}
	cp2, lag2, st2, err := finalize()
	if err != nil {
		t.Fatalf("second finalize: %v", err)
	}

	if cp1.Sequence != cp2.Sequence {
		t.Fatalf("checkpoint determinism mismatch: %d vs %d", cp1.Sequence, cp2.Sequence)
	}
	if lag1.LagEvents != lag2.LagEvents {
		t.Fatalf("lag determinism mismatch: %d vs %d", lag1.LagEvents, lag2.LagEvents)
	}
	if st1.Metrics.AppendEvents != st2.Metrics.AppendEvents || st1.Metrics.AppendNoops != st2.Metrics.AppendNoops {
		t.Fatalf("metrics determinism mismatch")
	}
}
