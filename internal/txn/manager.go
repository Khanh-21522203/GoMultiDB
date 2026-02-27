package txn

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
)

type State int

const (
	Created State = iota
	Pending
	Committing
	Committed
	Aborting
	Aborted
)

type IntentApplier interface {
	ApplyIntents(ctx context.Context, txnID [16]byte, commitHT uint64, limit int) (done bool, err error)
	RemoveIntents(ctx context.Context, txnID [16]byte, limit int) (done bool, err error)
}

type Record struct {
	TxnID         ids.TxnID
	State         State
	CommitHT      uint64
	LastHeartbeat time.Time
	RequestID     ids.RequestID
}

type Config struct {
	TxnTimeout      time.Duration
	NowFn           func() time.Time
	ApplyBatchLimit int
}

type Manager struct {
	mu      sync.Mutex
	records map[ids.TxnID]Record
	cfg     Config
	applier IntentApplier
}

func NewManager(cfg Config, applier IntentApplier) *Manager {
	if cfg.TxnTimeout <= 0 {
		cfg.TxnTimeout = 120 * time.Second
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() time.Time { return time.Now().UTC() }
	}
	if cfg.ApplyBatchLimit <= 0 {
		cfg.ApplyBatchLimit = 1000
	}
	return &Manager{records: make(map[ids.TxnID]Record), cfg: cfg, applier: applier}
}

func (m *Manager) Begin(_ context.Context, txnID ids.TxnID, reqID ids.RequestID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rec, ok := m.records[txnID]; ok {
		if rec.RequestID == reqID {
			return nil
		}
		return dberrors.New(dberrors.ErrIdempotencyConflict, "txn already exists with different request id", false, nil)
	}
	m.records[txnID] = Record{TxnID: txnID, State: Pending, LastHeartbeat: m.cfg.NowFn(), RequestID: reqID}
	return nil
}

func (m *Manager) Heartbeat(_ context.Context, txnID ids.TxnID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.records[txnID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "txn not found", false, nil)
	}
	if rec.State == Committed || rec.State == Aborted {
		return dberrors.New(dberrors.ErrInvalidArgument, "txn is terminal", false, nil)
	}
	rec.LastHeartbeat = m.cfg.NowFn()
	m.records[txnID] = rec
	return nil
}

func (m *Manager) Commit(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID, commitHT uint64) (uint64, error) {
	m.mu.Lock()
	rec, ok := m.records[txnID]
	if !ok {
		m.mu.Unlock()
		return 0, dberrors.New(dberrors.ErrInvalidArgument, "txn not found", false, nil)
	}
	if rec.RequestID != reqID {
		m.mu.Unlock()
		return 0, dberrors.New(dberrors.ErrIdempotencyConflict, "idempotency mismatch", false, nil)
	}
	if m.isExpiredLocked(rec) {
		rec.State = Aborted
		m.records[txnID] = rec
		m.mu.Unlock()
		return 0, dberrors.New(dberrors.ErrTimeout, "txn expired", true, nil)
	}

	switch rec.State {
	case Committed:
		ht := rec.CommitHT
		m.mu.Unlock()
		return ht, nil
	case Aborted, Aborting:
		m.mu.Unlock()
		return 0, dberrors.New(dberrors.ErrConflict, "txn aborted", false, nil)
	case Pending, Committing:
		rec.State = Committing
		rec.CommitHT = commitHT
		m.records[txnID] = rec
		m.mu.Unlock()
	default:
		m.mu.Unlock()
		return 0, dberrors.New(dberrors.ErrInvalidArgument, "invalid txn state for commit", false, nil)
	}

	if m.applier != nil {
		for {
			done, err := m.applier.ApplyIntents(ctx, txnID, commitHT, m.cfg.ApplyBatchLimit)
			if err != nil {
				m.mu.Lock()
				rec2 := m.records[txnID]
				rec2.State = Aborted
				m.records[txnID] = rec2
				m.mu.Unlock()
				return 0, dberrors.New(dberrors.ErrRetryableUnavailable, "apply intents failed", true, err)
			}
			if done {
				break
			}
		}
	}

	m.mu.Lock()
	rec = m.records[txnID]
	rec.State = Committed
	rec.CommitHT = commitHT
	m.records[txnID] = rec
	m.mu.Unlock()
	return commitHT, nil
}

func (m *Manager) Abort(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID) error {
	m.mu.Lock()
	rec, ok := m.records[txnID]
	if !ok {
		m.mu.Unlock()
		return dberrors.New(dberrors.ErrInvalidArgument, "txn not found", false, nil)
	}
	if rec.RequestID != reqID {
		m.mu.Unlock()
		return dberrors.New(dberrors.ErrIdempotencyConflict, "idempotency mismatch", false, nil)
	}

	switch rec.State {
	case Aborted:
		m.mu.Unlock()
		return nil
	case Committed:
		m.mu.Unlock()
		return dberrors.New(dberrors.ErrConflict, "txn already committed", false, nil)
	case Pending, Aborting, Committing:
		rec.State = Aborting
		m.records[txnID] = rec
		m.mu.Unlock()
	default:
		m.mu.Unlock()
		return dberrors.New(dberrors.ErrInvalidArgument, "invalid txn state for abort", false, nil)
	}

	if m.applier != nil {
		for {
			done, err := m.applier.RemoveIntents(ctx, txnID, m.cfg.ApplyBatchLimit)
			if err != nil {
				return dberrors.New(dberrors.ErrRetryableUnavailable, "remove intents failed", true, err)
			}
			if done {
				break
			}
		}
	}

	m.mu.Lock()
	rec = m.records[txnID]
	rec.State = Aborted
	m.records[txnID] = rec
	m.mu.Unlock()
	return nil
}

func (m *Manager) ExpireStale() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := 0
	for id, rec := range m.records {
		if rec.State == Pending || rec.State == Committing || rec.State == Aborting {
			if m.isExpiredLocked(rec) {
				rec.State = Aborted
				m.records[id] = rec
				count++
			}
		}
	}
	return count
}

func (m *Manager) isExpiredLocked(rec Record) bool {
	if m.cfg.TxnTimeout <= 0 {
		return false
	}
	return m.cfg.NowFn().Sub(rec.LastHeartbeat) > m.cfg.TxnTimeout
}

func (m *Manager) Get(txnID ids.TxnID) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[txnID]
	return rec, ok
}
