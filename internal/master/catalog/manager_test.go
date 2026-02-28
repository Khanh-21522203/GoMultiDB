package catalog

import (
	"context"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
)

func TestCreateTableRequiresLeader(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	_, err = m.CreateTable(context.Background(), CreateTableRequest{
		RequestID:   ids.RequestID("req-1"),
		NamespaceID: "ns-1",
		Name:        "users",
	})
	if err == nil {
		t.Fatalf("expected not leader error")
	}
	if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %s", n.Code)
	}
}

func TestCreateTableIdempotentByRequestID(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)

	id1, err := m.CreateTable(context.Background(), CreateTableRequest{
		RequestID:   ids.RequestID("req-1"),
		NamespaceID: "ns-1",
		Name:        "users",
	})
	if err != nil {
		t.Fatalf("create table first: %v", err)
	}
	id2, err := m.CreateTable(context.Background(), CreateTableRequest{
		RequestID:   ids.RequestID("req-1"),
		NamespaceID: "ns-1",
		Name:        "users",
	})
	if err != nil {
		t.Fatalf("create table retry: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("expected same table id on idempotent retry, got %s vs %s", id1, id2)
	}
}

func TestCreateTableSameRequestIDDifferentPayloadRejected(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)

	_, err = m.CreateTable(context.Background(), CreateTableRequest{
		RequestID:   ids.RequestID("req-1"),
		NamespaceID: "ns-1",
		Name:        "users",
	})
	if err != nil {
		t.Fatalf("create table first: %v", err)
	}
	_, err = m.CreateTable(context.Background(), CreateTableRequest{
		RequestID:   ids.RequestID("req-1"),
		NamespaceID: "ns-1",
		Name:        "accounts",
	})
	if err == nil {
		t.Fatalf("expected idempotency conflict")
	}
	if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %s", n.Code)
	}
}

func TestCreateTableAndGetTable(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)

	id, err := m.CreateTable(context.Background(), CreateTableRequest{
		RequestID:   ids.RequestID("req-1"),
		NamespaceID: "ns-1",
		Name:        "users",
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	tbl, err := m.GetTable(context.Background(), id)
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if tbl.State != TableRunning {
		t.Fatalf("expected table running state, got %v", tbl.State)
	}
	if tbl.CreateReqID != ids.RequestID("req-1") {
		t.Fatalf("unexpected create request id: %s", tbl.CreateReqID)
	}
}

func TestApplyTabletReportRequiresLeader(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	err = m.ApplyTabletReport(context.Background(), TabletReportDelta{TSUUID: "ts-1"})
	if err == nil {
		t.Fatalf("expected not leader error")
	}
	if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %s", n.Code)
	}
}

type testSink struct {
	got TabletReportDelta
}

func (s *testSink) ApplyTabletReport(_ context.Context, delta TabletReportDelta) error {
	s.got = delta
	return nil
}

func TestApplyTabletReportDelegatesToSink(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)

	sink := &testSink{}
	m.SetReconcileSink(sink)

delta := TabletReportDelta{
		TSUUID:        "ts-1",
		IsIncremental: true,
		SequenceNo:    7,
		Updated:       []string{"tablet-a"},
		RemovedIDs:    []string{"tablet-b"},
	}
	if err := m.ApplyTabletReport(context.Background(), delta); err != nil {
		t.Fatalf("apply tablet report: %v", err)
	}
	if sink.got.TSUUID != "ts-1" || sink.got.SequenceNo != 7 {
		t.Fatalf("unexpected sink delta: %+v", sink.got)
	}
}

func TestTableTransitionGuard(t *testing.T) {
	tbl := TableInfo{State: TablePreparing, Version: 1}
	if err := transitionTableState(&tbl, TableRunning); err != nil {
		t.Fatalf("preparing->running should be allowed: %v", err)
	}
	if err := transitionTableState(&tbl, TableDeleted); err == nil {
		t.Fatalf("running->deleted should be rejected")
	}
}
