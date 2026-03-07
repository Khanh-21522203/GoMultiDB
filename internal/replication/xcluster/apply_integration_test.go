package xcluster

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"GoMultiDB/internal/replication/cdc"
)

type applyRecorder struct {
	applied []cdc.Event
}

func (r *applyRecorder) Apply(_ context.Context, ev cdc.Event) error {
	r.applied = append(r.applied, ev)
	return nil
}

type failFirstCheckpointStore struct {
	inner     CheckpointStore
	failFirst bool
}

func (s *failFirstCheckpointStore) AdvanceCheckpoint(ctx context.Context, cp cdc.Checkpoint) error {
	if s.failFirst {
		s.failFirst = false
		return errors.New("injected checkpoint write failure")
	}
	return s.inner.AdvanceCheckpoint(ctx, cp)
}

func (s *failFirstCheckpointStore) GetCheckpoint(ctx context.Context, streamID, tabletID string) (cdc.Checkpoint, error) {
	return s.inner.GetCheckpoint(ctx, streamID, tabletID)
}

func TestIntegrationCDCToXClusterApplyReducesLagToZero(t *testing.T) {
	ctx := context.Background()
	store := cdc.NewStore()

	for i := 1; i <= 5; i++ {
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s-int", TabletID: "t-int", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}

	recorder := &applyRecorder{}
	loop, err := NewLoop(Config{}, store, recorder)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	before, err := loop.LagSnapshot(ctx, "s-int", "t-int")
	if err != nil {
		t.Fatalf("lag before apply: %v", err)
	}
	if before.LagEvents != 5 {
		t.Fatalf("expected lag 5 before apply, got %d", before.LagEvents)
	}

	poll, err := store.Poll(ctx, cdc.PollRequest{StreamID: "s-int", TabletID: "t-int", AfterSeq: 0, MaxRecords: 10})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if err := loop.ApplyBatch(ctx, poll.Events); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	after, err := loop.LagSnapshot(ctx, "s-int", "t-int")
	if err != nil {
		t.Fatalf("lag after apply: %v", err)
	}
	if after.LagEvents != 0 {
		t.Fatalf("expected lag 0 after apply, got %d", after.LagEvents)
	}
}

func TestIntegrationRestartContinuityWithFileCheckpointStore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "checkpoints.json")

	fileStore, err := cdc.NewFileCheckpointStore(cpPath)
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	recorder := &applyRecorder{}
	loop, err := NewLoop(Config{}, fileStore, recorder)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	if err := loop.ApplyEvent(ctx, cdc.Event{StreamID: "s-r", TabletID: "t-r", Sequence: 3, TimestampUTC: time.Now().UTC()}); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	reloadedStore, err := cdc.NewFileCheckpointStore(cpPath)
	if err != nil {
		t.Fatalf("reload file store: %v", err)
	}
	loop2, err := NewLoop(Config{}, reloadedStore, &applyRecorder{})
	if err != nil {
		t.Fatalf("new loop2: %v", err)
	}
	cp, err := loop2.ResumeCheckpoint(ctx, "s-r", "t-r")
	if err != nil {
		t.Fatalf("resume checkpoint: %v", err)
	}
	if cp.Sequence != 3 {
		t.Fatalf("expected resumed sequence 3, got %d", cp.Sequence)
	}
}

func TestIntegrationFailureThenRecoveryCheckpointAdvance(t *testing.T) {
	ctx := context.Background()
	baseStore := cdc.NewStore()
	failingStore := &failFirstCheckpointStore{inner: baseStore, failFirst: true}
	recorder := &applyRecorder{}
	loop, err := NewLoop(Config{Retry: RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}}, failingStore, recorder)
	if err != nil {
		t.Fatalf("new loop: %v", err)
	}

	ev := cdc.Event{StreamID: "s-f", TabletID: "t-f", Sequence: 1, TimestampUTC: time.Now().UTC()}
	if err := loop.ApplyEvent(ctx, ev); err == nil {
		t.Fatalf("expected first apply to fail due to injected checkpoint failure")
	}

	if err := loop.ApplyEvent(ctx, ev); err != nil {
		t.Fatalf("expected second apply to recover, got: %v", err)
	}
	cp, err := baseStore.GetCheckpoint(ctx, "s-f", "t-f")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 1 {
		t.Fatalf("expected checkpoint sequence 1 after recovery, got %d", cp.Sequence)
	}
}
