package pggate

import (
	"context"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
)

func TestSessionOpenIdempotentByRequestID(t *testing.T) {
	m := NewManager()
	id1, err := m.OpenSession(context.Background(), ids.RequestID("req-1"))
	if err != nil {
		t.Fatalf("open session first: %v", err)
	}
	id2, err := m.OpenSession(context.Background(), ids.RequestID("req-1"))
	if err != nil {
		t.Fatalf("open session second: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("expected idempotent session id, got %s vs %s", id1, id2)
	}
}

func TestQueueAndFlushWrites(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-2"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "table-1", Operation: "INSERT"}); err != nil {
		t.Fatalf("queue write: %v", err)
	}
	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "table-1", Operation: "UPDATE"}); err != nil {
		t.Fatalf("queue write 2: %v", err)
	}

	flushed, err := m.FlushWrites(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("flush writes: %v", err)
	}
	if len(flushed) != 2 {
		t.Fatalf("expected 2 flushed writes, got %d", len(flushed))
	}

	again, err := m.FlushWrites(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("flush writes second: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("expected empty flush after drain, got %d", len(again))
	}
}

func TestInvalidateTableCache(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-3"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	m.mu.Lock()
	m.sessions[sessionID].TableCache["users"] = TableDesc{TableID: "table-users", CatalogVersion: 1}
	m.mu.Unlock()

	if err := m.InvalidateTableCache(context.Background(), sessionID, 2); err != nil {
		t.Fatalf("invalidate cache: %v", err)
	}

	s, err := m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if s.CatalogVersion != 2 {
		t.Fatalf("expected catalog version 2, got %d", s.CatalogVersion)
	}
	if len(s.TableCache) != 0 {
		t.Fatalf("expected empty table cache after invalidation, got %d", len(s.TableCache))
	}
}

func TestBeginTxnCommitTxnClearsPendingWrites(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-4"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	txn, err := m.BeginTxn(context.Background(), sessionID, ids.RequestID("txn-1"))
	if err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if txn.State != TxnStateActive {
		t.Fatalf("expected active txn state, got %s", txn.State)
	}

	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "table-1", Operation: "INSERT"}); err != nil {
		t.Fatalf("queue write in txn: %v", err)
	}

	s, err := m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if s.Txn == nil {
		t.Fatalf("expected txn to be attached to session")
	}
	if s.Txn.OpSeq != 1 {
		t.Fatalf("expected txn op seq to advance to 1, got %d", s.Txn.OpSeq)
	}

	if err := m.CommitTxn(context.Background(), sessionID); err != nil {
		t.Fatalf("commit txn: %v", err)
	}

	s, err = m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session post commit: %v", err)
	}
	if s.Txn != nil {
		t.Fatalf("expected txn to be cleared after commit")
	}
	if len(s.PendingWrites) != 0 {
		t.Fatalf("expected pending writes cleared after commit, got %d", len(s.PendingWrites))
	}
}

func TestBeginTxnIdempotentWhenActive(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-5"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	txn1, err := m.BeginTxn(context.Background(), sessionID, ids.RequestID("txn-1"))
	if err != nil {
		t.Fatalf("begin txn first: %v", err)
	}
	txn2, err := m.BeginTxn(context.Background(), sessionID, ids.RequestID("txn-2"))
	if err != nil {
		t.Fatalf("begin txn second: %v", err)
	}
	if txn1.TxnID != txn2.TxnID {
		t.Fatalf("expected idempotent begin while active, got %s vs %s", txn1.TxnID, txn2.TxnID)
	}
}

func TestCommitTxnWithoutActiveTxnReturnsConflict(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-6"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	err = m.CommitTxn(context.Background(), sessionID)
	if err == nil {
		t.Fatalf("expected conflict when committing without active txn")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}
}

func TestAbortTxnClearsStateAndIsIdempotent(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-7"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if _, err := m.BeginTxn(context.Background(), sessionID, ids.RequestID("txn-1")); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "table-1", Operation: "UPDATE"}); err != nil {
		t.Fatalf("queue write: %v", err)
	}

	if err := m.AbortTxn(context.Background(), sessionID); err != nil {
		t.Fatalf("abort txn first: %v", err)
	}
	if err := m.AbortTxn(context.Background(), sessionID); err != nil {
		t.Fatalf("abort txn second: %v", err)
	}

	s, err := m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if s.Txn != nil {
		t.Fatalf("expected txn cleared after abort")
	}
	if len(s.PendingWrites) != 0 {
		t.Fatalf("expected pending writes cleared after abort, got %d", len(s.PendingWrites))
	}
}

func TestSavepointCreateRollbackAndRelease(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-8"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if _, err := m.BeginTxn(context.Background(), sessionID, ids.RequestID("txn-1")); err != nil {
		t.Fatalf("begin txn: %v", err)
	}

	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "table-1", Operation: "INSERT"}); err != nil {
		t.Fatalf("queue write 1: %v", err)
	}
	if err := m.CreateSavepoint(context.Background(), sessionID, "sp1"); err != nil {
		t.Fatalf("create savepoint: %v", err)
	}
	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "table-1", Operation: "UPDATE"}); err != nil {
		t.Fatalf("queue write 2: %v", err)
	}
	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "table-1", Operation: "DELETE"}); err != nil {
		t.Fatalf("queue write 3: %v", err)
	}

	if err := m.RollbackToSavepoint(context.Background(), sessionID, "sp1"); err != nil {
		t.Fatalf("rollback savepoint: %v", err)
	}
	s, err := m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session after rollback: %v", err)
	}
	if len(s.PendingWrites) != 1 {
		t.Fatalf("expected 1 write after rollback, got %d", len(s.PendingWrites))
	}
	if s.Txn == nil || s.Txn.OpSeq != 1 {
		t.Fatalf("expected txn op seq rolled back to 1")
	}

	if err := m.ReleaseSavepoint(context.Background(), sessionID, "sp1"); err != nil {
		t.Fatalf("release savepoint: %v", err)
	}
	s, err = m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session after release: %v", err)
	}
	if s.Txn == nil {
		t.Fatalf("expected active txn")
	}
	if len(s.Txn.Savepoints) != 0 {
		t.Fatalf("expected savepoints to be empty after release, got %d", len(s.Txn.Savepoints))
	}
}

func TestSavepointRequiresActiveTxn(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("req-9"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	err = m.CreateSavepoint(context.Background(), sessionID, "sp")
	if err == nil {
		t.Fatalf("expected conflict when creating savepoint without active txn")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}
}
