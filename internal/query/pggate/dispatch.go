// dispatch.go — interfaces and wiring for real tablet I/O and transaction
// coordination in the PgGate bridge.
//
// Design (docs/plans/plan-pggate-bridge.md §§10a–10c):
//
//   - PartitionResolver maps (tableID, bindVars) → tabletID.
//   - TabletDispatcher sends write batches and read requests to tablet leaders.
//   - TxnCoordinator wraps the distributed transaction manager (txn.Manager).
//
// When these are not injected (nil), the Manager falls back to the existing
// in-memory stub behaviour so all existing tests continue to pass.
package pggate

import (
	"context"
	"encoding/hex"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
)

// ── Interfaces ────────────────────────────────────────────────────────────────

// PartitionResolver maps a table+bind variables to the responsible tablet ID.
type PartitionResolver interface {
	// Resolve returns the tabletID that owns the given table + partition key.
	Resolve(ctx context.Context, tableID string, bindVars []PgValue) (tabletID string, err error)
}

// WriteResult is the response from a tablet write RPC.
type WriteResult struct {
	AppliedCount int
}

// ReadResult is the response from a tablet read RPC.
type ReadResult struct {
	RowCount int
}

// TabletDispatcher dispatches write batches and read requests to tablet leaders.
type TabletDispatcher interface {
	// TabletWrite sends a batch of write ops to the given tablet.
	TabletWrite(ctx context.Context, tabletID string, txnID string, ops []WriteOp) (WriteResult, error)
	// TabletRead sends a read request to the given tablet.
	TabletRead(ctx context.Context, tabletID string, txnID string, op ReadOp, snapshotHT uint64) (ReadResult, error)
}

// TxnCoordinator is the interface the pggate Manager uses to interact with
// the distributed transaction coordinator.
type TxnCoordinator interface {
	// Begin starts a new transaction and returns its ID.
	Begin(ctx context.Context, reqID ids.RequestID) (txnID ids.TxnID, startHT uint64, err error)
	// Commit commits the transaction identified by txnID.
	Commit(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID, commitHT uint64) (uint64, error)
	// Abort aborts the transaction.
	Abort(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID) error
}

// ── Manager wiring ────────────────────────────────────────────────────────────

// SetTabletDispatcher injects the tablet I/O dispatcher.
func (m *Manager) SetTabletDispatcher(d TabletDispatcher) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tabletDispatcher = d
}

// SetPartitionResolver injects the partition resolver.
func (m *Manager) SetPartitionResolver(r PartitionResolver) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.partitionResolver = r
}

// SetTxnCoordinator injects the distributed transaction coordinator.
func (m *Manager) SetTxnCoordinator(c TxnCoordinator) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txnCoordinator = c
}

// ── Real FlushWrites ──────────────────────────────────────────────────────────

// flushWritesReal dispatches pending writes to tablet leaders.
// Called by FlushWrites when a TabletDispatcher and PartitionResolver are set.
func (m *Manager) flushWritesReal(ctx context.Context, s *Session, ops []WriteOp) error {
	// Group writes by tabletID.
	groups := make(map[string][]WriteOp)
	for _, op := range ops {
		tabletID, err := m.partitionResolver.Resolve(ctx, op.TableID, op.BindVars)
		if err != nil {
			return dberrors.New(dberrors.ErrInternal, "resolve tablet for write", true, err)
		}
		groups[tabletID] = append(groups[tabletID], op)
	}

	txnID := ""
	if s.Txn != nil {
		txnID = s.Txn.TxnID
	}

	for tabletID, groupOps := range groups {
		result, err := m.tabletDispatcher.TabletWrite(ctx, tabletID, txnID, groupOps)
		if err != nil {
			m.incWriteRestartConflict()
			return err
		}
		_ = result
	}
	return nil
}

// ── Real ExecRead ─────────────────────────────────────────────────────────────

// execReadReal dispatches a read to the tablet leader.
// Called by ExecRead when a TabletDispatcher and PartitionResolver are set.
func (m *Manager) execReadReal(ctx context.Context, s *Session, op ReadOp) (int, error) {
	tabletID, err := m.partitionResolver.Resolve(ctx, op.TableID, op.BindVars)
	if err != nil {
		return 0, dberrors.New(dberrors.ErrInternal, "resolve tablet for read", true, err)
	}

	txnID := ""
	var snapshotHT uint64
	if s.Txn != nil {
		txnID = s.Txn.TxnID
		snapshotHT = s.Txn.SnapshotHT
	}

	result, err := m.tabletDispatcher.TabletRead(ctx, tabletID, txnID, op, snapshotHT)
	if err != nil {
		m.incReadRestartConflict()
		return 0, err
	}
	return result.RowCount, nil
}

// ── Real BeginTxn / CommitTxn / AbortTxn ────────────────────────────────────

// beginTxnReal calls the coordinator to begin a distributed transaction.
func (m *Manager) beginTxnReal(ctx context.Context, s *Session, reqID ids.RequestID) (*TxnHandle, error) {
	txnID, startHT, err := m.txnCoordinator.Begin(ctx, reqID)
	if err != nil {
		return nil, err
	}
	handle := &TxnHandle{
		TxnID:      txnID.String(),
		State:      TxnStateActive,
		Epoch:      1,
		OpSeq:      0,
		SnapshotHT: startHT,
		Savepoints: make([]Savepoint, 0),
	}
	s.Txn = handle
	return handle, nil
}

// commitTxnReal flushes remaining writes then commits via coordinator.
func (m *Manager) commitTxnReal(ctx context.Context, sessionID string, s *Session) error {
	if len(s.PendingWrites) > 0 {
		ops := append([]WriteOp(nil), s.PendingWrites...)
		if err := m.flushWritesReal(ctx, s, ops); err != nil {
			return err
		}
	}
	txnID, err := parseTxnIDHex(s.Txn.TxnID)
	if err != nil {
		return err
	}
	reqID, err := ids.NewRequestID()
	if err != nil {
		return dberrors.New(dberrors.ErrInternal, "generate commit reqID", false, err)
	}

	if _, err := m.txnCoordinator.Commit(ctx, txnID, reqID, 0); err != nil {
		return err
	}
	m.mu.Lock()
	if sess, ok := m.sessions[sessionID]; ok {
		sess.Txn = nil
		sess.PendingWrites = sess.PendingWrites[:0]
	}
	m.mu.Unlock()
	return nil
}

// abortTxnReal aborts the transaction via coordinator.
func (m *Manager) abortTxnReal(ctx context.Context, sessionID string, s *Session) error {
	txnID, err := parseTxnIDHex(s.Txn.TxnID)
	if err != nil {
		return err
	}
	reqID, err := ids.NewRequestID()
	if err != nil {
		return dberrors.New(dberrors.ErrInternal, "generate abort reqID", false, err)
	}

	if err := m.txnCoordinator.Abort(ctx, txnID, reqID); err != nil {
		return err
	}
	m.mu.Lock()
	if sess, ok := m.sessions[sessionID]; ok {
		sess.Txn = nil
		sess.PendingWrites = sess.PendingWrites[:0]
	}
	m.mu.Unlock()
	return nil
}

func parseTxnIDHex(txnIDHex string) (ids.TxnID, error) {
	var txnID ids.TxnID
	if txnIDHex == "" {
		return txnID, dberrors.New(dberrors.ErrInvalidArgument, "empty txn id", false, nil)
	}
	b, err := hex.DecodeString(txnIDHex)
	if err != nil {
		return txnID, dberrors.New(dberrors.ErrInvalidArgument, "invalid txn id hex", false, err)
	}
	if len(b) != len(txnID) {
		return txnID, dberrors.New(dberrors.ErrInvalidArgument, "invalid txn id length", false, nil)
	}
	copy(txnID[:], b)
	return txnID, nil
}
