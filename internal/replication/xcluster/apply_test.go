package xcluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/replication/cdc"
)

type applierStub struct {
	mu        sync.Mutex
	applyCall int
	failFirst int
	events    []cdc.Event
}

func (a *applierStub) Apply(_ context.Context, ev cdc.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applyCall++
	if a.failFirst > 0 {
		a.failFirst--
		return errors.New("transient apply failure")
	}
	a.events = append(a.events, ev)
	return nil
}

func (a *applierStub) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.applyCall
}

func TestApplyEventAdvancesCheckpoint(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{}
	loop, err := NewLoop(Config{}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	ev := cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC(), Payload: []byte("x")}
	if err := loop.ApplyEvent(context.Background(), ev); err != nil {
		t.Fatalf("apply event: %v", err)
	}
	cp, err := store.GetCheckpoint(context.Background(), "s1", "t1")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 1 {
		t.Fatalf("expected checkpoint sequence 1, got %d", cp.Sequence)
	}
	if applier.Calls() != 1 {
		t.Fatalf("expected one apply call, got %d", applier.Calls())
	}
}

func TestApplyEventIsIdempotentForDuplicateSequence(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{}
	loop, err := NewLoop(Config{}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	ev := cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: 1, TimestampUTC: time.Now().UTC()}

	if err := loop.ApplyEvent(context.Background(), ev); err != nil {
		t.Fatalf("apply first: %v", err)
	}
	if err := loop.ApplyEvent(context.Background(), ev); err != nil {
		t.Fatalf("apply duplicate: %v", err)
	}
	if applier.Calls() != 1 {
		t.Fatalf("expected one underlying apply call for duplicate event, got %d", applier.Calls())
	}

	stats, err := loop.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.DuplicateEvents == 0 {
		t.Fatalf("expected duplicate events counter > 0")
	}
}

func TestApplyEventRetriesTransientFailure(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{failFirst: 2}
	loop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 3, Backoff: 1 * time.Millisecond}}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	ev := cdc.Event{StreamID: "s2", TabletID: "t2", Sequence: 5, TimestampUTC: time.Now().UTC()}

	if err := loop.ApplyEvent(context.Background(), ev); err != nil {
		t.Fatalf("expected eventual success after retries, got: %v", err)
	}
	if applier.Calls() != 3 {
		t.Fatalf("expected three apply calls due to retries, got %d", applier.Calls())
	}

	stats, err := loop.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.RetryCount != 2 {
		t.Fatalf("expected retry count 2, got %d", stats.RetryCount)
	}
}

func TestApplyEventFailsWhenRetriesExhausted(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{failFirst: 10}
	loop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 2, Backoff: 1 * time.Millisecond}}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}
	ev := cdc.Event{StreamID: "s3", TabletID: "t3", Sequence: 1, TimestampUTC: time.Now().UTC()}

	err = loop.ApplyEvent(context.Background(), ev)
	if err == nil {
		t.Fatalf("expected exhausted retries error")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable, got %s", n.Code)
	}

	stats, err := loop.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.FailureCount != 1 {
		t.Fatalf("expected failure count 1, got %d", stats.FailureCount)
	}
}

func TestApplyBatchAtLeastOnceProgression(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{failFirst: 1}
	loop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 3, Backoff: 1 * time.Millisecond}}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	events := []cdc.Event{
		{StreamID: "s4", TabletID: "t4", Sequence: 1, TimestampUTC: time.Now().UTC()},
		{StreamID: "s4", TabletID: "t4", Sequence: 2, TimestampUTC: time.Now().UTC()},
		{StreamID: "s4", TabletID: "t4", Sequence: 2, TimestampUTC: time.Now().UTC()}, // duplicate
		{StreamID: "s4", TabletID: "t4", Sequence: 3, TimestampUTC: time.Now().UTC()},
	}

	if err := loop.ApplyBatch(context.Background(), events); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	cp, err := store.GetCheckpoint(context.Background(), "s4", "t4")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 3 {
		t.Fatalf("expected checkpoint sequence 3, got %d", cp.Sequence)
	}

	status, err := loop.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Config.Retry.MaxAttempts != 3 {
		t.Fatalf("expected retry max attempts 3, got %d", status.Config.Retry.MaxAttempts)
	}
	if status.Stats.AppliedEvents == 0 {
		t.Fatalf("expected applied events > 0")
	}
	if status.Stats.LastUpdated.IsZero() {
		t.Fatalf("expected non-zero last updated in status")
	}

	resumed, err := loop.ResumeCheckpoint(context.Background(), "s4", "t4")
	if err != nil {
		t.Fatalf("resume checkpoint: %v", err)
	}
	if resumed.Sequence != 3 {
		t.Fatalf("expected resumed sequence 3, got %d", resumed.Sequence)
	}

	lag, err := loop.LagSnapshot(context.Background(), "s4", "t4")
	if err != nil {
		t.Fatalf("lag snapshot: %v", err)
	}
	if lag.LagEvents != 0 {
		t.Fatalf("expected lag 0 after apply progression, got %d", lag.LagEvents)
	}
}

func TestApplyEventChaosReorderThenRecovery(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{}
	loop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 2, Backoff: 1 * time.Millisecond}}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s5", TabletID: "t5", Sequence: 2, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply seq2 first: %v", err)
	}
	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s5", TabletID: "t5", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply reordered seq1 should be treated as duplicate/noop: %v", err)
	}
	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s5", TabletID: "t5", Sequence: 3, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply seq3: %v", err)
	}

	cp, err := store.GetCheckpoint(context.Background(), "s5", "t5")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 3 {
		t.Fatalf("expected checkpoint sequence 3 after reorder recovery, got %d", cp.Sequence)
	}
}

func TestApplyEventRepeatedTransientThenTerminalFailure(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{failFirst: 100}
	loop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 3, Backoff: 1 * time.Millisecond}}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	err = loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s6", TabletID: "t6", Sequence: 1, TimestampUTC: time.Now().UTC()})
	if err == nil {
		t.Fatalf("expected exhausted retry failure")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable, got %s", n.Code)
	}

	stats, err := loop.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.FailureCount != 1 {
		t.Fatalf("expected failure count 1, got %d", stats.FailureCount)
	}
	if stats.RetryCount != 2 {
		t.Fatalf("expected retry count 2 (maxAttempts-1), got %d", stats.RetryCount)
	}
}

func TestApplyEventOwnershipTransferMonotonicSequenceGuarantee(t *testing.T) {
	store := cdc.NewStore()
	applier := &applierStub{}
	loop, err := NewLoop(Config{}, store, applier)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	// Before ownership transfer.
	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s-own", TabletID: "t-own", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply seq1: %v", err)
	}
	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s-own", TabletID: "t-own", Sequence: 2, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply seq2: %v", err)
	}

	// After ownership transfer, regressed sequence numbers are treated as duplicates.
	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s-own", TabletID: "t-own", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply regressed seq1 after transfer: %v", err)
	}

	// Progress resumes only when sequence continues monotonically.
	if err := loop.ApplyEvent(context.Background(), cdc.Event{StreamID: "s-own", TabletID: "t-own", Sequence: 3, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply seq3 after transfer: %v", err)
	}
	cp, err := store.GetCheckpoint(context.Background(), "s-own", "t-own")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 3 {
		t.Fatalf("expected checkpoint sequence 3 after ownership transfer, got %d", cp.Sequence)
	}
}
