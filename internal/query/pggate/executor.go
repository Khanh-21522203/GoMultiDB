package pggate

import (
	"context"
	"fmt"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/idempotency"
	"GoMultiDB/internal/common/ids"
)

type ExecResponse struct {
	Applied  bool
	Writes   int
	Rows     int
	TxnID    string
	InTxn    bool
	Flushed  bool
	OpSeq    uint64
	Conflict bool
}

type RetryStats struct {
	ReadRestartConflicts  uint64
	WriteRestartConflicts uint64
}

func (m *Manager) ExecWrite(ctx context.Context, sessionID string, op WriteOp, flush bool) (ExecResponse, error) {
	return m.ExecWriteWithRequest(ctx, sessionID, op, flush, "")
}

func (m *Manager) ExecWriteWithRequest(ctx context.Context, sessionID string, op WriteOp, flush bool, reqID ids.RequestID) (ExecResponse, error) {
	select {
	case <-ctx.Done():
		return ExecResponse{}, ctx.Err()
	default:
	}
	if sessionID == "" {
		return ExecResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "session id is required", false, nil)
	}
	if op.TableID == "" || op.Operation == "" {
		return ExecResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "table id and operation are required", false, nil)
	}

	if reqID != "" {
		if err := m.markIdempotentWrite(sessionID, reqID, op, flush); err != nil {
			return ExecResponse{}, err
		}
	}

	if op.Operation == "RETRY_CONFLICT" {
		m.incWriteRestartConflict()
		return ExecResponse{Conflict: true}, dberrors.New(dberrors.ErrConflict, "write restart conflict", true, nil)
	}

	if err := m.QueueWrite(ctx, sessionID, op); err != nil {
		return ExecResponse{}, err
	}

	s, err := m.GetSession(ctx, sessionID)
	if err != nil {
		return ExecResponse{}, err
	}

	resp := ExecResponse{Applied: true, Writes: 1, InTxn: s.Txn != nil}
	if s.Txn != nil {
		resp.TxnID = s.Txn.TxnID
		resp.OpSeq = s.Txn.OpSeq
	}

	if !flush {
		return resp, nil
	}

	flushed, err := m.FlushWrites(ctx, sessionID)
	if err != nil {
		return ExecResponse{}, err
	}
	resp.Flushed = true
	resp.Writes = len(flushed)
	return resp, nil
}

func (m *Manager) ExecRead(ctx context.Context, sessionID string, op ReadOp) (ExecResponse, error) {
	return m.ExecReadWithRequest(ctx, sessionID, op, "")
}

func (m *Manager) ExecReadWithRequest(ctx context.Context, sessionID string, op ReadOp, reqID ids.RequestID) (ExecResponse, error) {
	select {
	case <-ctx.Done():
		return ExecResponse{}, ctx.Err()
	default:
	}
	if sessionID == "" {
		return ExecResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "session id is required", false, nil)
	}
	if op.TableID == "" {
		return ExecResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "table id is required", false, nil)
	}
	if reqID != "" {
		if err := m.markIdempotentRead(sessionID, reqID, op); err != nil {
			return ExecResponse{}, err
		}
	}
	if op.IndexID == "RETRY_CONFLICT" {
		m.incReadRestartConflict()
		return ExecResponse{Conflict: true}, dberrors.New(dberrors.ErrConflict, "read restart conflict", true, nil)
	}

	s, err := m.GetSession(ctx, sessionID)
	if err != nil {
		return ExecResponse{}, err
	}

	rows := op.FetchSize
	if rows <= 0 {
		rows = 1
	}
	resp := ExecResponse{Applied: true, Rows: rows, InTxn: s.Txn != nil}
	if s.Txn != nil {
		resp.TxnID = s.Txn.TxnID
		resp.OpSeq = s.Txn.OpSeq
	}
	return resp, nil
}

func (m *Manager) RetryStats(ctx context.Context) (RetryStats, error) {
	select {
	case <-ctx.Done():
		return RetryStats{}, ctx.Err()
	default:
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.retryStats, nil
}

func (m *Manager) incReadRestartConflict() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retryStats.ReadRestartConflicts++
}

func (m *Manager) incWriteRestartConflict() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retryStats.WriteRestartConflicts++
}

func writeFingerprint(op WriteOp, flush bool) string {
	return fmt.Sprintf("%s|%s|%t|%d", op.TableID, op.Operation, flush, len(op.Columns))
}

func readFingerprint(op ReadOp) string {
	return fmt.Sprintf("%s|%s|%d|%d|%d", op.TableID, op.IndexID, op.FetchSize, len(op.BindVars), len(op.Targets))
}

func (m *Manager) markIdempotentWrite(sessionID string, reqID ids.RequestID, op WriteOp, flush bool) error {
	if m.idemStore == nil {
		return nil
	}
	scope := "pggate.execwrite." + sessionID
	fingerprint := writeFingerprint(op, flush)
	seen, prev, err := m.idemStore.Seen(scope, reqID)
	if err != nil {
		return err
	}
	if seen {
		if prev != fingerprint {
			return dberrors.New(dberrors.ErrIdempotencyConflict, "idempotent write fingerprint mismatch", false, nil)
		}
		return nil
	}
	return m.idemStore.Mark(scope, reqID, fingerprint, 2*time.Minute)
}

func (m *Manager) markIdempotentRead(sessionID string, reqID ids.RequestID, op ReadOp) error {
	if m.idemStore == nil {
		return nil
	}
	scope := "pggate.execread." + sessionID
	fingerprint := readFingerprint(op)
	seen, prev, err := m.idemStore.Seen(scope, reqID)
	if err != nil {
		return err
	}
	if seen {
		if prev != fingerprint {
			return dberrors.New(dberrors.ErrIdempotencyConflict, "idempotent read fingerprint mismatch", false, nil)
		}
		return nil
	}
	return m.idemStore.Mark(scope, reqID, fingerprint, 2*time.Minute)
}

func (m *Manager) SetIdempotencyStore(store idempotency.Store) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idemStore = store
}
