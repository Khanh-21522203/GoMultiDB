package cdc

import (
	"context"
	"errors"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

type producerStub struct {
	resp ProducerReadResponse
	err  error
}

func (p *producerStub) Read(_ context.Context, _ ProducerReadRequest) (ProducerReadResponse, error) {
	if p.err != nil {
		return ProducerReadResponse{}, p.err
	}
	return p.resp, nil
}

func TestServiceCreateDeleteAndListStreams(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()

	if err := svc.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := svc.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("idempotent create stream: %v", err)
	}
	ids, err := svc.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if len(ids) != 1 || ids[0] != "s1" {
		t.Fatalf("unexpected stream list: %+v", ids)
	}

	if err := svc.DeleteStream(ctx, "s1"); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	ids, _ = svc.ListStreams(ctx)
	if len(ids) != 0 {
		t.Fatalf("expected empty stream list after delete")
	}
}

func TestServiceGetChangesAndSetCheckpoint(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()

	if err := svc.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if err := store.AppendEvent(ctx, Event{StreamID: "s1", TabletID: "t1", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	out, err := svc.GetChanges(ctx, GetChangesRequest{StreamID: "s1", AfterSeq: 1, MaxRecords: 10})
	if err != nil {
		t.Fatalf("get changes: %v", err)
	}
	if len(out.Events) != 2 || out.LatestSeen != 3 {
		t.Fatalf("unexpected get changes result: len=%d latest=%d", len(out.Events), out.LatestSeen)
	}
	if err := svc.SetCheckpoint(ctx, Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: out.LatestSeen}); err != nil {
		t.Fatalf("set checkpoint: %v", err)
	}
	cp, err := store.GetCheckpoint(ctx, "s1", "t1")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 3 {
		t.Fatalf("expected checkpoint 3, got %d", cp.Sequence)
	}
}

func TestServiceCanonicalErrors(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()

	_, err = svc.GetChanges(ctx, GetChangesRequest{})
	if err == nil {
		t.Fatalf("expected invalid argument for empty stream id")
	}
	if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}

	err = svc.SetCheckpoint(ctx, Checkpoint{StreamID: "missing", TabletID: "t1", Sequence: 1})
	if err == nil {
		t.Fatalf("expected invalid argument for missing stream")
	}
	if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}
}

func TestServiceProducerReplayAndFault(t *testing.T) {
	store := NewStore()
	svc, err := NewService(store)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()
	if err := svc.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	stub := &producerStub{resp: ProducerReadResponse{Events: []Event{{StreamID: "s1", TabletID: "t1", Sequence: 11}}, LatestSeen: 11}}
	if err := svc.SetProducer(stub); err != nil {
		t.Fatalf("set producer: %v", err)
	}

	out, err := svc.GetChanges(ctx, GetChangesRequest{StreamID: "s1", AfterSeq: 10, MaxRecords: 10})
	if err != nil {
		t.Fatalf("get changes via stub producer: %v", err)
	}
	if out.LatestSeen != 11 || len(out.Events) != 1 || out.Events[0].Sequence != 11 {
		t.Fatalf("unexpected producer response: %+v", out)
	}

	stub.err = errors.New("producer failure")
	_, err = svc.GetChanges(ctx, GetChangesRequest{StreamID: "s1", AfterSeq: 11, MaxRecords: 10})
	if err == nil {
		t.Fatalf("expected producer failure")
	}
}
