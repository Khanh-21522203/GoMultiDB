package pggate

import (
	"context"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/idempotency"
	"GoMultiDB/internal/common/ids"
)

func TestExecWriteQueuesAndOptionalFlush(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("exec-1"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	resp, err := m.ExecWrite(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "INSERT"}, false)
	if err != nil {
		t.Fatalf("exec write no flush: %v", err)
	}
	if !resp.Applied || resp.Flushed {
		t.Fatalf("expected applied and not flushed")
	}

	s, err := m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if len(s.PendingWrites) != 1 {
		t.Fatalf("expected one pending write, got %d", len(s.PendingWrites))
	}

	resp, err = m.ExecWrite(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "UPDATE"}, true)
	if err != nil {
		t.Fatalf("exec write flush: %v", err)
	}
	if !resp.Flushed {
		t.Fatalf("expected flushed response")
	}
	if resp.Writes != 2 {
		t.Fatalf("expected 2 flushed writes, got %d", resp.Writes)
	}
}

func TestExecWriteWithTxnIncludesTxnMetadata(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("exec-2"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if _, err := m.BeginTxn(context.Background(), sessionID, ids.RequestID("txn-1")); err != nil {
		t.Fatalf("begin txn: %v", err)
	}

	resp, err := m.ExecWrite(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "INSERT"}, false)
	if err != nil {
		t.Fatalf("exec write in txn: %v", err)
	}
	if !resp.InTxn || resp.TxnID == "" {
		t.Fatalf("expected txn metadata in response")
	}
	if resp.OpSeq != 1 {
		t.Fatalf("expected op seq=1, got %d", resp.OpSeq)
	}
}

func TestExecReadValidationAndFetchSize(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("exec-3"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	_, err = m.ExecRead(context.Background(), sessionID, ReadOp{})
	if err == nil {
		t.Fatalf("expected invalid argument for missing table id")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}

	resp, err := m.ExecRead(context.Background(), sessionID, ReadOp{TableID: "t1", FetchSize: 5})
	if err != nil {
		t.Fatalf("exec read: %v", err)
	}
	if resp.Rows != 5 {
		t.Fatalf("expected rows=5, got %d", resp.Rows)
	}
}

func TestExecConflictIncrementsRetryStats(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("exec-4"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	_, err = m.ExecWrite(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "RETRY_CONFLICT"}, false)
	if err == nil {
		t.Fatalf("expected write conflict error")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code on write, got %s", n.Code)
	}

	_, err = m.ExecRead(context.Background(), sessionID, ReadOp{TableID: "t1", IndexID: "RETRY_CONFLICT"})
	if err == nil {
		t.Fatalf("expected read conflict error")
	}
	n = dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code on read, got %s", n.Code)
	}

	stats, err := m.RetryStats(context.Background())
	if err != nil {
		t.Fatalf("retry stats: %v", err)
	}
	if stats.WriteRestartConflicts != 1 {
		t.Fatalf("expected write conflict count 1, got %d", stats.WriteRestartConflicts)
	}
	if stats.ReadRestartConflicts != 1 {
		t.Fatalf("expected read conflict count 1, got %d", stats.ReadRestartConflicts)
	}
}

func TestExecWriteIdempotencyFingerprintMismatch(t *testing.T) {
	m := NewManager()
	m.SetIdempotencyStore(idempotency.NewMemoryStore())
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("exec-5"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	reqID := ids.RequestID("rid-1")

	_, err = m.ExecWriteWithRequest(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "INSERT"}, false, reqID)
	if err != nil {
		t.Fatalf("first idempotent write: %v", err)
	}

	_, err = m.ExecWriteWithRequest(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "UPDATE"}, false, reqID)
	if err == nil {
		t.Fatalf("expected idempotency conflict on fingerprint mismatch")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrIdempotencyConflict {
		t.Fatalf("expected idempotency conflict code, got %s", n.Code)
	}
}

func TestExecReadIdempotencyFingerprintMismatch(t *testing.T) {
	m := NewManager()
	m.SetIdempotencyStore(idempotency.NewMemoryStore())
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("exec-6"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	reqID := ids.RequestID("rid-read-1")

	_, err = m.ExecReadWithRequest(context.Background(), sessionID, ReadOp{TableID: "t1", IndexID: "idx1", FetchSize: 5}, reqID)
	if err != nil {
		t.Fatalf("first idempotent read: %v", err)
	}

	_, err = m.ExecReadWithRequest(context.Background(), sessionID, ReadOp{TableID: "t1", IndexID: "idx2", FetchSize: 5}, reqID)
	if err == nil {
		t.Fatalf("expected idempotency conflict on read fingerprint mismatch")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrIdempotencyConflict {
		t.Fatalf("expected idempotency conflict code, got %s", n.Code)
	}
}
