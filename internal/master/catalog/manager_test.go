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

func TestCreateTableDurableIdempotencyAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	m1, err := NewManager(store)
	if err != nil {
		t.Fatalf("new manager1: %v", err)
	}
	m1.SetLeader(true)
	id1, err := m1.CreateTable(ctx, CreateTableRequest{
		RequestID:   ids.RequestID("req-durable"),
		NamespaceID: "ns-1",
		Name:        "users",
	})
	if err != nil {
		t.Fatalf("create table first: %v", err)
	}

	m2, err := NewManager(store)
	if err != nil {
		t.Fatalf("new manager2: %v", err)
	}
	m2.SetLeader(true)
	id2, err := m2.CreateTable(ctx, CreateTableRequest{
		RequestID:   ids.RequestID("req-durable"),
		NamespaceID: "ns-1",
		Name:        "users",
	})
	if err != nil {
		t.Fatalf("create table retry after restart: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("durable idempotency mismatch: %s vs %s", id1, id2)
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

// ── GetTableByName ────────────────────────────────────────────────────────────

func TestGetTableByName(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	_, err = m.CreateTable(ctx, CreateTableRequest{
		RequestID:   ids.RequestID("req-gbn"),
		NamespaceID: "ns-1",
		Name:        "products",
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl, err := m.GetTableByName(ctx, "ns-1", "products")
	if err != nil {
		t.Fatalf("GetTableByName: %v", err)
	}
	if tbl.Name != "products" {
		t.Fatalf("name: want products, got %s", tbl.Name)
	}
}

func TestGetTableByNameNotFound(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	_, err = m.GetTableByName(context.Background(), "ns-1", "ghost")
	if err == nil {
		t.Fatalf("expected error for unknown table")
	}
	if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected ErrInvalidArgument, got %s", n.Code)
	}
}

// ── AlterTable ────────────────────────────────────────────────────────────────

func TestAlterTable(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	tableID, err := m.CreateTable(ctx, CreateTableRequest{
		RequestID:   ids.RequestID("req-alt-1"),
		NamespaceID: "ns-1",
		Name:        "orders",
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	tbl, _ := m.GetTable(ctx, tableID)
	vBefore := tbl.Version

	if err := m.AlterTable(ctx, AlterTableRequest{
		RequestID: ids.RequestID("req-alt-2"),
		TableID:   tableID,
	}); err != nil {
		t.Fatalf("AlterTable: %v", err)
	}
	tbl, _ = m.GetTable(ctx, tableID)
	if tbl.Version <= vBefore {
		t.Fatalf("expected version bump after AlterTable, got %d (was %d)", tbl.Version, vBefore)
	}
	if tbl.State != TableRunning {
		t.Fatalf("expected Running state after AlterTable, got %v", tbl.State)
	}
}

func TestAlterTableIdempotent(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	tableID, _ := m.CreateTable(ctx, CreateTableRequest{RequestID: "req-a1", NamespaceID: "ns", Name: "t"})
	err = m.AlterTable(ctx, AlterTableRequest{RequestID: "req-a2", TableID: tableID})
	if err != nil {
		t.Fatalf("first AlterTable: %v", err)
	}
	// Same reqID → idempotent
	err = m.AlterTable(ctx, AlterTableRequest{RequestID: "req-a2", TableID: tableID})
	if err != nil {
		t.Fatalf("idempotent AlterTable: %v", err)
	}
}

func TestAlterTableRequiresLeader(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	err = m.AlterTable(context.Background(), AlterTableRequest{RequestID: "req", TableID: "t"})
	if err == nil {
		t.Fatalf("expected ErrNotLeader")
	}
	if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %s", n.Code)
	}
}

// ── DeleteTable ───────────────────────────────────────────────────────────────

func TestDeleteTable(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	tableID, err := m.CreateTable(ctx, CreateTableRequest{RequestID: "req-del-1", NamespaceID: "ns", Name: "legacy"})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := m.DeleteTable(ctx, DeleteTableRequest{RequestID: "req-del-2", TableID: tableID}); err != nil {
		t.Fatalf("DeleteTable: %v", err)
	}

	tbl, _ := m.GetTable(ctx, tableID)
	if tbl.State != TableDeleting {
		t.Fatalf("expected Deleting state, got %v", tbl.State)
	}
}

func TestDeleteTableTombstonesRunningTablets(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	tableID, _ := m.CreateTable(ctx, CreateTableRequest{RequestID: "req-dt-1", NamespaceID: "ns", Name: "big"})

	// Register two tablets.
	_, err = m.CreateTablet(ctx, TabletInfo{TableID: tableID, State: TabletRunning}, ids.RequestID("req-dt-t1"))
	if err != nil {
		t.Fatalf("CreateTablet 1: %v", err)
	}
	_, err = m.CreateTablet(ctx, TabletInfo{TableID: tableID, State: TabletRunning}, ids.RequestID("req-dt-t2"))
	if err != nil {
		t.Fatalf("CreateTablet 2: %v", err)
	}

	if err := m.DeleteTable(ctx, DeleteTableRequest{RequestID: "req-dt-2", TableID: tableID}); err != nil {
		t.Fatalf("DeleteTable: %v", err)
	}

	// Both tablets should now be tombstoned.
	for tid, ti := range m.snap.Tablets {
		if ti.TableID != tableID {
			continue
		}
		if ti.State != TabletTombstoned {
			t.Fatalf("tablet %s: expected Tombstoned, got %v", tid, ti.State)
		}
	}
}

// ── CreateTablet ──────────────────────────────────────────────────────────────

func TestCreateTablet(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	tableID, _ := m.CreateTable(ctx, CreateTableRequest{RequestID: "req-ct-1", NamespaceID: "ns", Name: "t"})
	tabletID, err := m.CreateTablet(ctx, TabletInfo{TableID: tableID}, ids.RequestID("req-ct-2"))
	if err != nil {
		t.Fatalf("CreateTablet: %v", err)
	}
	if tabletID == "" {
		t.Fatalf("expected non-empty tablet id")
	}

	// Idempotent.
	tabletID2, err := m.CreateTablet(ctx, TabletInfo{TableID: tableID}, ids.RequestID("req-ct-2"))
	if err != nil {
		t.Fatalf("idempotent CreateTablet: %v", err)
	}
	if tabletID2 != tabletID {
		t.Fatalf("expected same tablet id on retry, got %s vs %s", tabletID, tabletID2)
	}
}

// ── ProcessTabletReport ───────────────────────────────────────────────────────

func TestProcessTabletReportFullReset(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	report := TabletReport{
		TSUUID:        "ts-1",
		IsIncremental: false,
		SequenceNo:    5,
		Tablets: []TabletInfo{
			{TabletID: ids.TabletID("tab-a"), State: TabletRunning},
		},
	}
	if err := m.ProcessTabletReport(ctx, report); err != nil {
		t.Fatalf("ProcessTabletReport: %v", err)
	}
	// Full report again with lower seq — should still apply (it's a reset).
	report.SequenceNo = 3
	report.Tablets = []TabletInfo{{TabletID: ids.TabletID("tab-b"), State: TabletRunning}}
	if err := m.ProcessTabletReport(ctx, report); err != nil {
		t.Fatalf("full reset with lower seq: %v", err)
	}
}

func TestProcessTabletReportStaleIncremental(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetLeader(true)
	ctx := context.Background()

	// Apply seq=10 incremental.
	if err := m.ProcessTabletReport(ctx, TabletReport{
		TSUUID:        "ts-2",
		IsIncremental: true,
		SequenceNo:    10,
		Tablets:       []TabletInfo{{TabletID: "tab-c", State: TabletRunning}},
	}); err != nil {
		t.Fatalf("seq=10: %v", err)
	}

	// Stale seq=5 incremental must be dropped.
	if err := m.ProcessTabletReport(ctx, TabletReport{
		TSUUID:        "ts-2",
		IsIncremental: true,
		SequenceNo:    5,
		Tablets:       []TabletInfo{{TabletID: "tab-c", State: TabletTombstoned}},
	}); err != nil {
		t.Fatalf("stale seq should be silently dropped, got err: %v", err)
	}

	// The tablet should still be Running (stale was dropped).
	ti, ok := m.snap.Tablets[ids.TabletID("tab-c")]
	if !ok {
		t.Fatalf("tab-c not found after stale drop")
	}
	if ti.State != TabletRunning {
		t.Fatalf("stale report must not overwrite, got state=%v", ti.State)
	}
}

func TestProcessTabletReportRequiresLeader(t *testing.T) {
	m, err := NewManager(NewMemoryStore())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := m.ProcessTabletReport(context.Background(), TabletReport{TSUUID: "ts"}); err == nil {
		t.Fatalf("expected ErrNotLeader")
	}
}
