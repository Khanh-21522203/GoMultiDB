package pggate

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/txn"
)

const defaultTabletShards = 16

func newDefaultBridge() (PartitionResolver, TabletDispatcher, TxnCoordinator) {
	return &localPartitionResolver{}, newLocalTabletDispatcher(), newLocalTxnCoordinator()
}

type localPartitionResolver struct{}

func (r *localPartitionResolver) Resolve(_ context.Context, tableID string, bindVars []PgValue) (string, error) {
	if strings.TrimSpace(tableID) == "" {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "table id is required", false, nil)
	}
	key := tableID
	if len(bindVars) > 0 {
		key = fmt.Sprintf("%s|%v", tableID, bindVars[0])
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	shard := h.Sum32() % defaultTabletShards
	return fmt.Sprintf("local-%s-%02d", sanitizeTableID(tableID), shard), nil
}

type localTabletDispatcher struct {
	mu       sync.RWMutex
	tableRow map[string]map[string]int
}

func newLocalTabletDispatcher() *localTabletDispatcher {
	return &localTabletDispatcher{
		tableRow: make(map[string]map[string]int),
	}
}

func (d *localTabletDispatcher) TabletWrite(_ context.Context, tabletID string, _ string, ops []WriteOp) (WriteResult, error) {
	if strings.TrimSpace(tabletID) == "" {
		return WriteResult{}, dberrors.New(dberrors.ErrInvalidArgument, "tablet id is required", false, nil)
	}
	if len(ops) == 0 {
		return WriteResult{AppliedCount: 0}, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	tableRows, ok := d.tableRow[tabletID]
	if !ok {
		tableRows = make(map[string]int)
		d.tableRow[tabletID] = tableRows
	}

	for _, op := range ops {
		if strings.TrimSpace(op.TableID) == "" {
			return WriteResult{}, dberrors.New(dberrors.ErrInvalidArgument, "table id is required for write op", false, nil)
		}
		current := tableRows[op.TableID]
		switch normalizeWriteOp(op.Operation) {
		case "INSERT":
			current++
		case "DELETE":
			if current > 0 {
				current--
			}
		case "UPDATE":
			if current == 0 {
				current = 1
			}
		default:
			return WriteResult{}, dberrors.New(dberrors.ErrInvalidArgument, "unsupported write operation", false, nil)
		}
		tableRows[op.TableID] = current
	}

	return WriteResult{AppliedCount: len(ops)}, nil
}

func (d *localTabletDispatcher) TabletRead(_ context.Context, tabletID string, _ string, op ReadOp, _ uint64) (ReadResult, error) {
	if strings.TrimSpace(tabletID) == "" {
		return ReadResult{}, dberrors.New(dberrors.ErrInvalidArgument, "tablet id is required", false, nil)
	}
	if strings.TrimSpace(op.TableID) == "" {
		return ReadResult{}, dberrors.New(dberrors.ErrInvalidArgument, "table id is required", false, nil)
	}

	d.mu.RLock()
	rowCount := 0
	if tableRows, ok := d.tableRow[tabletID]; ok {
		rowCount = tableRows[op.TableID]
	}
	d.mu.RUnlock()

	if rowCount == 0 {
		// Keep deterministic behavior for empty-table reads until a storage adapter
		// is injected: surface at least one row, or the requested fetch size.
		if op.FetchSize > 0 {
			rowCount = op.FetchSize
		} else {
			rowCount = 1
		}
	}
	if op.FetchSize > 0 && rowCount > op.FetchSize {
		rowCount = op.FetchSize
	}

	return ReadResult{RowCount: rowCount}, nil
}

type localTxnCoordinator struct {
	mu          sync.Mutex
	manager     *txn.Manager
	requestByTx map[string]ids.RequestID
}

func newLocalTxnCoordinator() *localTxnCoordinator {
	return &localTxnCoordinator{
		manager:     txn.NewManager(txn.Config{}, nil),
		requestByTx: make(map[string]ids.RequestID),
	}
}

func (c *localTxnCoordinator) Begin(ctx context.Context, reqID ids.RequestID) (ids.TxnID, uint64, error) {
	if reqID == "" {
		return ids.TxnID{}, 0, dberrors.New(dberrors.ErrInvalidArgument, "request id is required", false, nil)
	}
	txnID, err := ids.NewTxnID()
	if err != nil {
		return ids.TxnID{}, 0, dberrors.New(dberrors.ErrInternal, "generate transaction id", false, err)
	}
	startHT := uint64(time.Now().UTC().UnixNano())
	if err := c.manager.Begin(ctx, txnID, reqID, txn.BeginOptions{
		StartHT:   startHT,
		Isolation: txn.SnapshotIsolation,
	}); err != nil {
		return ids.TxnID{}, 0, err
	}
	c.mu.Lock()
	c.requestByTx[txnID.String()] = reqID
	c.mu.Unlock()
	return txnID, startHT, nil
}

func (c *localTxnCoordinator) Commit(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID, commitHT uint64) (uint64, error) {
	commitReqID := c.requestForTxn(txnID, reqID)
	if commitReqID == "" {
		return 0, dberrors.New(dberrors.ErrInvalidArgument, "request id is required", false, nil)
	}
	if commitHT == 0 {
		commitHT = uint64(time.Now().UTC().UnixNano())
	}
	return c.manager.Commit(ctx, txnID, commitReqID, commitHT)
}

func (c *localTxnCoordinator) Abort(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID) error {
	abortReqID := c.requestForTxn(txnID, reqID)
	if abortReqID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "request id is required", false, nil)
	}
	err := c.manager.Abort(ctx, txnID, abortReqID)
	if err == nil {
		c.mu.Lock()
		delete(c.requestByTx, txnID.String())
		c.mu.Unlock()
	}
	return err
}

func (c *localTxnCoordinator) requestForTxn(txnID ids.TxnID, fallback ids.RequestID) ids.RequestID {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reqID, ok := c.requestByTx[txnID.String()]; ok {
		return reqID
	}
	return fallback
}

func normalizeWriteOp(op string) string {
	return strings.ToUpper(strings.TrimSpace(op))
}

func sanitizeTableID(tableID string) string {
	var b strings.Builder
	for _, r := range tableID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	if b.Len() == 0 {
		return "table"
	}
	return b.String()
}
