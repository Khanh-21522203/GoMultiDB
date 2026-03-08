package pggate_test

import (
	"context"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/query/pggate"
)

// ── mock implementations ──────────────────────────────────────────────────────

type staticResolver struct {
	tabletID string
	err      error
}

func (r *staticResolver) Resolve(_ context.Context, _ string, _ []pggate.PgValue) (string, error) {
	return r.tabletID, r.err
}

type recordingDispatcher struct {
	writeCalls atomic.Int32
	readCalls  atomic.Int32
	writeErr   error
	readErr    error
	readRows   int
}

func (d *recordingDispatcher) TabletWrite(_ context.Context, _, _ string, ops []pggate.WriteOp) (pggate.WriteResult, error) {
	d.writeCalls.Add(1)
	return pggate.WriteResult{AppliedCount: len(ops)}, d.writeErr
}

func (d *recordingDispatcher) TabletRead(_ context.Context, _, _ string, op pggate.ReadOp, _ uint64) (pggate.ReadResult, error) {
	d.readCalls.Add(1)
	rows := d.readRows
	if rows == 0 {
		rows = op.FetchSize
		if rows <= 0 {
			rows = 1
		}
	}
	return pggate.ReadResult{RowCount: rows}, d.readErr
}

type recordingCoordinator struct {
	beginCalls     atomic.Int32
	commitCalls    atomic.Int32
	abortCalls     atomic.Int32
	beginErr       error
	commitErr      error
	abortErr       error
	lastCommitTxn  ids.TxnID
	lastAbortTxn   ids.TxnID
}

func (c *recordingCoordinator) Begin(_ context.Context, _ ids.RequestID) (ids.TxnID, uint64, error) {
	c.beginCalls.Add(1)
	if c.beginErr != nil {
		return ids.TxnID{}, 0, c.beginErr
	}
	return ids.MustNewTxnID(), 100, nil
}

func (c *recordingCoordinator) Commit(_ context.Context, txnID ids.TxnID, _ ids.RequestID, _ uint64) (uint64, error) {
	c.commitCalls.Add(1)
	c.lastCommitTxn = txnID
	return 200, c.commitErr
}

func (c *recordingCoordinator) Abort(_ context.Context, txnID ids.TxnID, _ ids.RequestID) error {
	c.abortCalls.Add(1)
	c.lastAbortTxn = txnID
	return c.abortErr
}

// ── setup helpers ─────────────────────────────────────────────────────────────

func newDispatchManager(d pggate.TabletDispatcher, r pggate.PartitionResolver, c pggate.TxnCoordinator) (*pggate.Manager, string) {
	m := pggate.NewManager()
	m.SetTabletDispatcher(d)
	m.SetPartitionResolver(r)
	if c != nil {
		m.SetTxnCoordinator(c)
	}
	sessionID, _ := m.OpenSession(context.Background(), ids.MustNewRequestID())
	return m, sessionID
}

// ── FlushWrites with real dispatch ────────────────────────────────────────────

func TestFlushWritesCallsTabletDispatcher(t *testing.T) {
	disp := &recordingDispatcher{}
	res := &staticResolver{tabletID: "tablet-1"}
	m, sessionID := newDispatchManager(disp, res, nil)
	ctx := context.Background()

	// Queue two writes.
	_ = m.QueueWrite(ctx, sessionID, pggate.WriteOp{TableID: "t1", Operation: "INSERT"})
	_ = m.QueueWrite(ctx, sessionID, pggate.WriteOp{TableID: "t1", Operation: "INSERT"})

	ops, err := m.FlushWrites(ctx, sessionID)
	if err != nil {
		t.Fatalf("FlushWrites: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops returned, got %d", len(ops))
	}
	if int(disp.writeCalls.Load()) != 1 {
		t.Fatalf("expected 1 TabletWrite call, got %d", disp.writeCalls.Load())
	}
}

func TestFlushWritesMultipleTablets(t *testing.T) {
	// Two tables → two different tablets.
	callCount := atomic.Int32{}
	resolver := &multiTableResolver{
		mapping: map[string]string{
			"t1": "tablet-A",
			"t2": "tablet-B",
		},
	}
	disp := &countingDispatcher{counter: &callCount}
	m, sessionID := newDispatchManager(disp, resolver, nil)
	ctx := context.Background()

	_ = m.QueueWrite(ctx, sessionID, pggate.WriteOp{TableID: "t1", Operation: "INSERT"})
	_ = m.QueueWrite(ctx, sessionID, pggate.WriteOp{TableID: "t2", Operation: "INSERT"})

	_, err := m.FlushWrites(ctx, sessionID)
	if err != nil {
		t.Fatalf("FlushWrites: %v", err)
	}
	// One write call per tablet.
	if int(callCount.Load()) != 2 {
		t.Fatalf("expected 2 TabletWrite calls (one per tablet), got %d", callCount.Load())
	}
}

type multiTableResolver struct{ mapping map[string]string }

func (r *multiTableResolver) Resolve(_ context.Context, tableID string, _ []pggate.PgValue) (string, error) {
	if tid, ok := r.mapping[tableID]; ok {
		return tid, nil
	}
	return "", errors.New("unknown table")
}

type countingDispatcher struct{ counter *atomic.Int32 }

func (d *countingDispatcher) TabletWrite(_ context.Context, _, _ string, ops []pggate.WriteOp) (pggate.WriteResult, error) {
	d.counter.Add(1)
	return pggate.WriteResult{AppliedCount: len(ops)}, nil
}

func (d *countingDispatcher) TabletRead(_ context.Context, _, _ string, op pggate.ReadOp, _ uint64) (pggate.ReadResult, error) {
	return pggate.ReadResult{RowCount: op.FetchSize}, nil
}

func TestFlushWritesDispatchError(t *testing.T) {
	disp := &recordingDispatcher{writeErr: errors.New("tablet unavailable")}
	res := &staticResolver{tabletID: "tablet-1"}
	m, sessionID := newDispatchManager(disp, res, nil)
	ctx := context.Background()

	_ = m.QueueWrite(ctx, sessionID, pggate.WriteOp{TableID: "t1", Operation: "INSERT"})
	_, err := m.FlushWrites(ctx, sessionID)
	if err == nil {
		t.Fatalf("expected error from dispatcher")
	}
}

func TestFlushWritesResolverError(t *testing.T) {
	disp := &recordingDispatcher{}
	res := &staticResolver{err: errors.New("partition not found")}
	m, sessionID := newDispatchManager(disp, res, nil)
	ctx := context.Background()

	_ = m.QueueWrite(ctx, sessionID, pggate.WriteOp{TableID: "t1", Operation: "INSERT"})
	_, err := m.FlushWrites(ctx, sessionID)
	if err == nil {
		t.Fatalf("expected error from resolver")
	}
}

// ── ExecRead with real dispatch ───────────────────────────────────────────────

func TestExecReadCallsTabletDispatcher(t *testing.T) {
	disp := &recordingDispatcher{readRows: 7}
	res := &staticResolver{tabletID: "tablet-1"}
	m, sessionID := newDispatchManager(disp, res, nil)
	ctx := context.Background()

	resp, err := m.ExecRead(ctx, sessionID, pggate.ReadOp{TableID: "t1", FetchSize: 10})
	if err != nil {
		t.Fatalf("ExecRead: %v", err)
	}
	if resp.Rows != 7 {
		t.Fatalf("expected 7 rows from tablet, got %d", resp.Rows)
	}
	if int(disp.readCalls.Load()) != 1 {
		t.Fatalf("expected 1 TabletRead call, got %d", disp.readCalls.Load())
	}
}

func TestExecReadDispatchError(t *testing.T) {
	disp := &recordingDispatcher{readErr: dberrors.New(dberrors.ErrConflict, "restart required", true, nil)}
	res := &staticResolver{tabletID: "tablet-1"}
	m, sessionID := newDispatchManager(disp, res, nil)

	_, err := m.ExecRead(context.Background(), sessionID, pggate.ReadOp{TableID: "t1", FetchSize: 5})
	if err == nil {
		t.Fatalf("expected error from tablet read")
	}
}

// ── BeginTxn with real coordinator ───────────────────────────────────────────

func TestBeginTxnCallsCoordinator(t *testing.T) {
	coord := &recordingCoordinator{}
	disp := &recordingDispatcher{}
	res := &staticResolver{tabletID: "t1"}
	m, sessionID := newDispatchManager(disp, res, coord)
	ctx := context.Background()

	_, err := m.BeginTxn(ctx, sessionID, ids.MustNewRequestID())
	if err != nil {
		t.Fatalf("BeginTxn: %v", err)
	}
	if int(coord.beginCalls.Load()) != 1 {
		t.Fatalf("expected 1 coordinator Begin call, got %d", coord.beginCalls.Load())
	}
}

func TestBeginTxnCoordinatorError(t *testing.T) {
	coord := &recordingCoordinator{beginErr: errors.New("coordinator unavailable")}
	disp := &recordingDispatcher{}
	res := &staticResolver{tabletID: "t1"}
	m, sessionID := newDispatchManager(disp, res, coord)

	_, err := m.BeginTxn(context.Background(), sessionID, ids.MustNewRequestID())
	if err == nil {
		t.Fatalf("expected error from coordinator begin")
	}
}

func TestCommitAndAbortUseDecodedTxnID(t *testing.T) {
	coord := &recordingCoordinator{}
	disp := &recordingDispatcher{}
	res := &staticResolver{tabletID: "tablet-1"}
	m, sessionID := newDispatchManager(disp, res, coord)
	ctx := context.Background()

	h, err := m.BeginTxn(ctx, sessionID, ids.MustNewRequestID())
	if err != nil {
		t.Fatalf("BeginTxn: %v", err)
	}
	expectedBytes, err := ids.NewTxnID()
	if err != nil {
		t.Fatalf("NewTxnID: %v", err)
	}
	_ = expectedBytes
	var expected ids.TxnID
	if _, err := hex.Decode(expected[:], []byte(h.TxnID)); err != nil {
		t.Fatalf("decode txn id: %v", err)
	}

	if err := m.CommitTxn(ctx, sessionID); err != nil {
		t.Fatalf("CommitTxn: %v", err)
	}
	if coord.lastCommitTxn != expected {
		t.Fatalf("commit txnID mismatch: got %x want %x", coord.lastCommitTxn, expected)
	}

	h2, err := m.BeginTxn(ctx, sessionID, ids.MustNewRequestID())
	if err != nil {
		t.Fatalf("BeginTxn #2: %v", err)
	}
	var expected2 ids.TxnID
	if _, err := hex.Decode(expected2[:], []byte(h2.TxnID)); err != nil {
		t.Fatalf("decode txn id #2: %v", err)
	}

	if err := m.AbortTxn(ctx, sessionID); err != nil {
		t.Fatalf("AbortTxn: %v", err)
	}
	if coord.lastAbortTxn != expected2 {
		t.Fatalf("abort txnID mismatch: got %x want %x", coord.lastAbortTxn, expected2)
	}
}

// ── stub fallback (no dispatcher injected) ────────────────────────────────────

func TestStubFallbackNoDispatcher(t *testing.T) {
	m := pggate.NewManager()
	sessionID, _ := m.OpenSession(context.Background(), ids.MustNewRequestID())
	ctx := context.Background()

	// Without dispatcher, FlushWrites returns ops in-memory.
	_ = m.QueueWrite(ctx, sessionID, pggate.WriteOp{TableID: "t1", Operation: "INSERT"})
	ops, err := m.FlushWrites(ctx, sessionID)
	if err != nil {
		t.Fatalf("stub FlushWrites: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 op from stub, got %d", len(ops))
	}

	// ExecRead returns stub row count.
	resp, err := m.ExecRead(ctx, sessionID, pggate.ReadOp{TableID: "t1", FetchSize: 5})
	if err != nil {
		t.Fatalf("stub ExecRead: %v", err)
	}
	if resp.Rows != 5 {
		t.Fatalf("expected stub rows=5, got %d", resp.Rows)
	}
}
