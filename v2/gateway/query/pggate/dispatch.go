// dispatch.go declares the interfaces and Manager wiring through which
// pggate reaches real tablet I/O and distributed transaction coordination.
//
//   - PartitionResolver maps (tableID, bindVars) to a tabletID.
//   - TabletDispatcher sends write batches and read requests to tablet
//     leaders.
//   - TxnCoordinator wraps the distributed transaction manager
//     (v2/engine/txn.Manager).
//
// When these are not injected, Manager's read/write/transaction methods
// report ErrRetryableUnavailable rather than falling back to an in-memory
// implementation.
package pggate

import (
	"context"

	"GoMultiDB/v2/contracts/ids"
)

// PartitionResolver maps a table and its bind variables to the tabletID
// responsible for that partition.
type PartitionResolver interface {
	// Resolve returns the tabletID that owns the given table + partition
	// key.
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

// TabletDispatcher dispatches write batches and read requests to tablet
// leaders.
type TabletDispatcher interface {
	// TabletWrite sends a batch of write ops to the given tablet.
	TabletWrite(ctx context.Context, tabletID string, txnID string, ops []WriteOp) (WriteResult, error)
	// TabletRead sends a read request to the given tablet.
	TabletRead(ctx context.Context, tabletID string, txnID string, op ReadOp, snapshotHT uint64) (ReadResult, error)
}

// TxnCoordinator is the interface Manager uses to interact with the
// distributed transaction coordinator.
type TxnCoordinator interface {
	// Begin starts a new transaction and returns its ID and start hybrid
	// timestamp.
	Begin(ctx context.Context, reqID ids.RequestID) (txnID ids.TxnID, startHT uint64, err error)
	// Commit commits the transaction identified by txnID.
	Commit(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID, commitHT uint64) (uint64, error)
	// Abort aborts the transaction identified by txnID.
	Abort(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID) error
}

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
