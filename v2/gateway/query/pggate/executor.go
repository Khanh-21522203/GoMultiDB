package pggate

import (
	"context"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/idempotency"
	"GoMultiDB/v2/contracts/ids"
)

// ExecResponse is the outcome of a single ExecWrite or ExecRead call.
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

// RetryStats aggregates read/write conflict counts observed while
// dispatching to tablets.
type RetryStats struct {
	ReadRestartConflicts  uint64
	WriteRestartConflicts uint64
}

// ExecWrite queues op on sessionID and, if flush is true, immediately
// flushes it to the configured TabletDispatcher.
//
// Not yet implemented in this scaffold.
func (m *Manager) ExecWrite(ctx context.Context, sessionID string, op WriteOp, flush bool) (ExecResponse, error) {
	return ExecResponse{}, errors.ErrNotImplemented
}

// ExecWriteWithRequest is ExecWrite with an explicit RequestID for
// idempotent-retry deduplication via the configured idempotency.Store.
//
// Not yet implemented in this scaffold.
func (m *Manager) ExecWriteWithRequest(ctx context.Context, sessionID string, op WriteOp, flush bool, reqID ids.RequestID) (ExecResponse, error) {
	return ExecResponse{}, errors.ErrNotImplemented
}

// ExecRead dispatches op to the tablet leader owning its partition via the
// configured TabletDispatcher and PartitionResolver.
//
// Not yet implemented in this scaffold.
func (m *Manager) ExecRead(ctx context.Context, sessionID string, op ReadOp) (ExecResponse, error) {
	return ExecResponse{}, errors.ErrNotImplemented
}

// ExecReadWithRequest is ExecRead with an explicit RequestID for
// idempotent-retry deduplication via the configured idempotency.Store.
//
// Not yet implemented in this scaffold.
func (m *Manager) ExecReadWithRequest(ctx context.Context, sessionID string, op ReadOp, reqID ids.RequestID) (ExecResponse, error) {
	return ExecResponse{}, errors.ErrNotImplemented
}

// RetryStats returns the aggregated read/write conflict statistics.
//
// Not yet implemented in this scaffold.
func (m *Manager) RetryStats(ctx context.Context) (RetryStats, error) {
	return RetryStats{}, errors.ErrNotImplemented
}

// SetIdempotencyStore injects the idempotency.Store used to deduplicate
// retried ExecWrite/ExecRead calls.
func (m *Manager) SetIdempotencyStore(store idempotency.Store) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.idemStore = store
}
