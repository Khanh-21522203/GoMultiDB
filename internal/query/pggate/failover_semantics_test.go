package pggate

import (
	"context"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/idempotency"
	"GoMultiDB/internal/common/ids"
)

func TestPhase5FailoverTxnStatePreservedOnConflict(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("failover-1"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	if _, err := m.BeginTxn(context.Background(), sessionID, ids.RequestID("txn-1")); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := m.QueueWrite(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "INSERT"}); err != nil {
		t.Fatalf("queue pre-conflict write: %v", err)
	}

	_, err = m.ExecWrite(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "RETRY_CONFLICT"}, false)
	if err == nil {
		t.Fatalf("expected write conflict")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}

	s, err := m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session after conflict: %v", err)
	}
	if s.Txn == nil {
		t.Fatalf("expected active txn to be preserved across conflict")
	}
	if len(s.PendingWrites) != 1 {
		t.Fatalf("expected pending writes preserved, got %d", len(s.PendingWrites))
	}

	stats, err := m.RetryStats(context.Background())
	if err != nil {
		t.Fatalf("retry stats: %v", err)
	}
	if stats.WriteRestartConflicts != 1 {
		t.Fatalf("expected write conflict stat=1, got %d", stats.WriteRestartConflicts)
	}
}

func TestPhase5FailoverReadConflictAndRetryRecovery(t *testing.T) {
	m := NewManager()
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("failover-2"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	_, err = m.ExecRead(context.Background(), sessionID, ReadOp{TableID: "t1", IndexID: "RETRY_CONFLICT", FetchSize: 10})
	if err == nil {
		t.Fatalf("expected read conflict")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}

	resp, err := m.ExecRead(context.Background(), sessionID, ReadOp{TableID: "t1", IndexID: "idx1", FetchSize: 10})
	if err != nil {
		t.Fatalf("retry read should succeed: %v", err)
	}
	if !resp.Applied || resp.Rows != 10 {
		t.Fatalf("expected successful retry read, got applied=%v rows=%d", resp.Applied, resp.Rows)
	}

	stats, err := m.RetryStats(context.Background())
	if err != nil {
		t.Fatalf("retry stats: %v", err)
	}
	if stats.ReadRestartConflicts != 1 {
		t.Fatalf("expected read conflict stat=1, got %d", stats.ReadRestartConflicts)
	}
}

func TestPhase5FailoverIdempotentWriteReplaySemantics(t *testing.T) {
	m := NewManager()
	m.SetIdempotencyStore(idempotency.NewMemoryStore())
	sessionID, err := m.OpenSession(context.Background(), ids.RequestID("failover-3"))
	if err != nil {
		t.Fatalf("open session: %v", err)
	}

	reqID := ids.RequestID("rid-failover")
	_, err = m.ExecWriteWithRequest(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "INSERT"}, false, reqID)
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	_, err = m.ExecWriteWithRequest(context.Background(), sessionID, WriteOp{TableID: "t1", Operation: "INSERT"}, false, reqID)
	if err != nil {
		t.Fatalf("idempotent replay should be accepted: %v", err)
	}

	s, err := m.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if len(s.PendingWrites) != 2 {
		t.Fatalf("expected 2 queued writes in skeleton replay semantics, got %d", len(s.PendingWrites))
	}
}
