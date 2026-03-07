package cdc

import (
	"context"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestStoreAppendPollOrdering(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	if err := s.AppendEvent(ctx, Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC(), Payload: []byte("a")}); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := s.AppendEvent(ctx, Event{StreamID: "s1", TabletID: "t1", Sequence: 2, TimestampUTC: time.Now().UTC(), Payload: []byte("b")}); err != nil {
		t.Fatalf("append 2: %v", err)
	}
	if err := s.AppendEvent(ctx, Event{StreamID: "s1", TabletID: "t1", Sequence: 3, TimestampUTC: time.Now().UTC(), Payload: []byte("c")}); err != nil {
		t.Fatalf("append 3: %v", err)
	}

	resp, err := s.Poll(ctx, PollRequest{StreamID: "s1", TabletID: "t1", AfterSeq: 1, MaxRecords: 10})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(resp.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(resp.Events))
	}
	if resp.Events[0].Sequence != 2 || resp.Events[1].Sequence != 3 {
		t.Fatalf("expected ordered sequences [2,3], got [%d,%d]", resp.Events[0].Sequence, resp.Events[1].Sequence)
	}
	if resp.LatestSeen != 3 {
		t.Fatalf("expected latest seen 3, got %d", resp.LatestSeen)
	}
}

func TestStoreAppendMonotonicValidation(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	if err := s.AppendEvent(ctx, Event{StreamID: "s1", TabletID: "t1", Sequence: 2, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("append baseline: %v", err)
	}

	err := s.AppendEvent(ctx, Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()})
	if err == nil {
		t.Fatalf("expected sequence conflict")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}

	if err := s.AppendEvent(ctx, Event{StreamID: "s1", TabletID: "t1", Sequence: 2, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("expected idempotent append on same sequence, got: %v", err)
	}

	status, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Metrics.AppendNoops == 0 {
		t.Fatalf("expected append noop metric > 0")
	}
}

func TestCheckpointMonotonicAdvance(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	if err := s.AdvanceCheckpoint(ctx, Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 10}); err != nil {
		t.Fatalf("advance 10: %v", err)
	}
	if err := s.AdvanceCheckpoint(ctx, Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 10}); err != nil {
		t.Fatalf("idempotent advance 10: %v", err)
	}

	err := s.AdvanceCheckpoint(ctx, Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 9})
	if err == nil {
		t.Fatalf("expected checkpoint regression conflict")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}

	cp, err := s.GetCheckpoint(ctx, "s1", "t1")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 10 {
		t.Fatalf("expected checkpoint sequence 10, got %d", cp.Sequence)
	}

	status, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Metrics.CheckpointAdvances != 1 {
		t.Fatalf("expected checkpoint advances 1, got %d", status.Metrics.CheckpointAdvances)
	}
	if status.Metrics.CheckpointNoops != 1 {
		t.Fatalf("expected checkpoint noops 1, got %d", status.Metrics.CheckpointNoops)
	}
}

func TestPollPagination(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		if err := s.AppendEvent(ctx, Event{StreamID: "s2", TabletID: "t2", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	resp, err := s.Poll(ctx, PollRequest{StreamID: "s2", TabletID: "t2", AfterSeq: 0, MaxRecords: 2})
	if err != nil {
		t.Fatalf("poll page1: %v", err)
	}
	if len(resp.Events) != 2 || resp.LatestSeen != 2 {
		t.Fatalf("expected page1 len=2 latest=2 got len=%d latest=%d", len(resp.Events), resp.LatestSeen)
	}

	resp, err = s.Poll(ctx, PollRequest{StreamID: "s2", TabletID: "t2", AfterSeq: resp.LatestSeen, MaxRecords: 2})
	if err != nil {
		t.Fatalf("poll page2: %v", err)
	}
	if len(resp.Events) != 2 || resp.LatestSeen != 4 {
		t.Fatalf("expected page2 len=2 latest=4 got len=%d latest=%d", len(resp.Events), resp.LatestSeen)
	}

	status, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Metrics.PollRequests != 2 {
		t.Fatalf("expected poll requests 2, got %d", status.Metrics.PollRequests)
	}
	if status.Metrics.PollReturnedEvents != 4 {
		t.Fatalf("expected poll returned events 4, got %d", status.Metrics.PollReturnedEvents)
	}
	if status.Streams == 0 || status.Tablets == 0 {
		t.Fatalf("expected non-zero streams/tablets in status")
	}
	if status.LastUpdated.IsZero() {
		t.Fatalf("expected non-zero last updated timestamp")
	}

	if err := s.AdvanceCheckpoint(ctx, Checkpoint{StreamID: "s2", TabletID: "t2", Sequence: 3}); err != nil {
		t.Fatalf("advance checkpoint for lag snapshot: %v", err)
	}
	lag, err := s.LagSnapshot(ctx, "s2", "t2")
	if err != nil {
		t.Fatalf("lag snapshot: %v", err)
	}
	if lag.LagEvents != 2 {
		t.Fatalf("expected lag events 2, got %d", lag.LagEvents)
	}
	if lag.CollectedAt.IsZero() {
		t.Fatalf("expected non-zero lag collected timestamp")
	}
}
