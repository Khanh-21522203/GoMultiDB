package pggate

import (
	"context"
	"fmt"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/idempotency"
	"GoMultiDB/internal/common/ids"
)

type PgValue any

type TableDesc struct {
	TableID        string
	CatalogVersion uint64
}

type ReadOp struct {
	TableID   string
	IndexID   string
	BindVars  []PgValue
	Targets   []int
	FetchSize int
}

type WriteOp struct {
	TableID   string
	Operation string // INSERT/UPDATE/DELETE
	Columns   map[int]PgValue
	// BindVars contains the partition key values used by PartitionResolver.
	// May be nil for non-partitioned or single-tablet tables.
	BindVars []PgValue
}

type TxnState string

const (
	TxnStateNone   TxnState = "NONE"
	TxnStateActive TxnState = "ACTIVE"
)

type Savepoint struct {
	Name       string
	WriteIndex int
	OpSeq      uint64
}

type TxnHandle struct {
	TxnID      string
	State      TxnState
	Epoch      uint64
	OpSeq      uint64
	SnapshotHT uint64 // hybrid timestamp at which the txn reads
	Savepoints []Savepoint
}

type Session struct {
	SessionID      string
	CatalogVersion uint64
	Txn            *TxnHandle
	TableCache     map[string]TableDesc
	PendingWrites  []WriteOp
}

type Manager struct {
	mu                sync.RWMutex
	sessions          map[string]*Session
	retryStats        RetryStats
	idemStore         idempotency.Store
	tabletDispatcher  TabletDispatcher
	partitionResolver PartitionResolver
	txnCoordinator    TxnCoordinator
}

func NewManager() *Manager {
	return &Manager{sessions: make(map[string]*Session)}
}

func (m *Manager) OpenSession(ctx context.Context, reqID ids.RequestID) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	if reqID == "" {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "request id is required", false, nil)
	}
	id := fmt.Sprintf("pggate-%s", reqID)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[id]; ok {
		return id, nil
	}
	m.sessions[id] = &Session{
		SessionID:      id,
		CatalogVersion: 1,
		Txn:            nil,
		TableCache:     make(map[string]TableDesc),
		PendingWrites:  make([]WriteOp, 0),
	}
	return id, nil
}

func (m *Manager) CloseSession(ctx context.Context, sessionID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if sessionID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "session id is required", false, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, sessionID)
	return nil
}

func (m *Manager) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	cp := *s
	cp.TableCache = cloneTableCache(s.TableCache)
	cp.PendingWrites = append([]WriteOp(nil), s.PendingWrites...)
	if s.Txn != nil {
		txn := *s.Txn
		txn.Savepoints = append([]Savepoint(nil), s.Txn.Savepoints...)
		cp.Txn = &txn
	}
	return &cp, nil
}

func (m *Manager) QueueWrite(ctx context.Context, sessionID string, op WriteOp) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if op.TableID == "" || op.Operation == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "table id and operation are required", false, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	s.PendingWrites = append(s.PendingWrites, op)
	if s.Txn != nil && s.Txn.State == TxnStateActive {
		s.Txn.OpSeq++
	}
	return nil
}

func (m *Manager) FlushWrites(ctx context.Context, sessionID string) ([]WriteOp, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	ops := append([]WriteOp(nil), s.PendingWrites...)
	dispatcher := m.tabletDispatcher
	resolver := m.partitionResolver
	m.mu.Unlock()

	// Real dispatch: if both dispatcher and resolver are injected, send to tablets.
	if dispatcher != nil && resolver != nil && len(ops) > 0 {
		m.mu.RLock()
		sessionCopy := m.sessions[sessionID]
		var sCopy *Session
		if sessionCopy != nil {
			cp := *sessionCopy
			if sessionCopy.Txn != nil {
				txn := *sessionCopy.Txn
				cp.Txn = &txn
			}
			sCopy = &cp
		}
		m.mu.RUnlock()
		if sCopy != nil {
			if err := m.flushWritesReal(ctx, sCopy, ops); err != nil {
				return nil, err
			}
		}
	}

	// Clear the buffer regardless.
	m.mu.Lock()
	if sess, ok2 := m.sessions[sessionID]; ok2 {
		sess.PendingWrites = sess.PendingWrites[:0]
	}
	m.mu.Unlock()
	return ops, nil
}

func (m *Manager) BeginTxn(ctx context.Context, sessionID string, reqID ids.RequestID) (*TxnHandle, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if sessionID == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "session id is required", false, nil)
	}
	if reqID == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "request id is required", false, nil)
	}

	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.Txn != nil && s.Txn.State == TxnStateActive {
		txn := *s.Txn
		txn.Savepoints = append([]Savepoint(nil), s.Txn.Savepoints...)
		m.mu.Unlock()
		return &txn, nil
	}
	coordinator := m.txnCoordinator
	m.mu.Unlock()

	// Real coordinator path.
	if coordinator != nil {
		m.mu.Lock()
		sess, ok2 := m.sessions[sessionID]
		if !ok2 {
			m.mu.Unlock()
			return nil, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
		}
		handle, err := m.beginTxnReal(ctx, sess, reqID)
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		txn := *handle
		return &txn, nil
	}

	// Stub path.
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok = m.sessions[sessionID]
	if !ok {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	s.Txn = &TxnHandle{
		TxnID:      fmt.Sprintf("%s-txn-%s", sessionID, reqID),
		State:      TxnStateActive,
		Epoch:      1,
		OpSeq:      0,
		Savepoints: make([]Savepoint, 0),
	}
	txn := *s.Txn
	txn.Savepoints = append([]Savepoint(nil), s.Txn.Savepoints...)
	return &txn, nil
}

func (m *Manager) CreateSavepoint(ctx context.Context, sessionID, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if sessionID == "" || name == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "session id and savepoint name are required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.Txn == nil || s.Txn.State != TxnStateActive {
		return dberrors.New(dberrors.ErrConflict, "no active transaction", false, nil)
	}

	for i := len(s.Txn.Savepoints) - 1; i >= 0; i-- {
		if s.Txn.Savepoints[i].Name == name {
			s.Txn.Savepoints = append(s.Txn.Savepoints[:i], s.Txn.Savepoints[i+1:]...)
			break
		}
	}

	s.Txn.Savepoints = append(s.Txn.Savepoints, Savepoint{
		Name:       name,
		WriteIndex: len(s.PendingWrites),
		OpSeq:      s.Txn.OpSeq,
	})
	return nil
}

func (m *Manager) RollbackToSavepoint(ctx context.Context, sessionID, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if sessionID == "" || name == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "session id and savepoint name are required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.Txn == nil || s.Txn.State != TxnStateActive {
		return dberrors.New(dberrors.ErrConflict, "no active transaction", false, nil)
	}

	idx := -1
	var sp Savepoint
	for i := len(s.Txn.Savepoints) - 1; i >= 0; i-- {
		if s.Txn.Savepoints[i].Name == name {
			idx = i
			sp = s.Txn.Savepoints[i]
			break
		}
	}
	if idx == -1 {
		return dberrors.New(dberrors.ErrInvalidArgument, "savepoint not found", false, nil)
	}

	if sp.WriteIndex < len(s.PendingWrites) {
		s.PendingWrites = s.PendingWrites[:sp.WriteIndex]
	}
	s.Txn.OpSeq = sp.OpSeq
	s.Txn.Savepoints = s.Txn.Savepoints[:idx+1]
	return nil
}

func (m *Manager) ReleaseSavepoint(ctx context.Context, sessionID, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if sessionID == "" || name == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "session id and savepoint name are required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.Txn == nil || s.Txn.State != TxnStateActive {
		return dberrors.New(dberrors.ErrConflict, "no active transaction", false, nil)
	}

	for i := len(s.Txn.Savepoints) - 1; i >= 0; i-- {
		if s.Txn.Savepoints[i].Name == name {
			s.Txn.Savepoints = append(s.Txn.Savepoints[:i], s.Txn.Savepoints[i+1:]...)
			return nil
		}
	}
	return dberrors.New(dberrors.ErrInvalidArgument, "savepoint not found", false, nil)
}

func (m *Manager) CommitTxn(ctx context.Context, sessionID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if sessionID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "session id is required", false, nil)
	}

	m.mu.RLock()
	s, ok := m.sessions[sessionID]
	coordinator := m.txnCoordinator
	dispatcher := m.tabletDispatcher
	m.mu.RUnlock()

	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.Txn == nil || s.Txn.State != TxnStateActive {
		return dberrors.New(dberrors.ErrConflict, "no active transaction", false, nil)
	}

	// Real coordinator commit.
	if coordinator != nil && dispatcher != nil {
		return m.commitTxnReal(ctx, sessionID, s)
	}

	// Stub path.
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok2 := m.sessions[sessionID]; ok2 {
		sess.Txn = nil
		sess.PendingWrites = sess.PendingWrites[:0]
	}
	return nil
}

func (m *Manager) AbortTxn(ctx context.Context, sessionID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if sessionID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "session id is required", false, nil)
	}

	m.mu.RLock()
	s, ok := m.sessions[sessionID]
	coordinator := m.txnCoordinator
	dispatcher := m.tabletDispatcher
	m.mu.RUnlock()

	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.Txn == nil || s.Txn.State != TxnStateActive {
		return nil
	}

	// Real coordinator abort.
	if coordinator != nil && dispatcher != nil {
		return m.abortTxnReal(ctx, sessionID, s)
	}

	// Stub path.
	m.mu.Lock()
	defer m.mu.Unlock()
	if sess, ok2 := m.sessions[sessionID]; ok2 {
		sess.Txn = nil
		sess.PendingWrites = sess.PendingWrites[:0]
	}
	return nil
}

func (m *Manager) InvalidateTableCache(ctx context.Context, sessionID string, newCatalogVersion uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if newCatalogVersion <= s.CatalogVersion {
		return nil
	}
	s.CatalogVersion = newCatalogVersion
	s.TableCache = make(map[string]TableDesc)
	return nil
}

func cloneTableCache(in map[string]TableDesc) map[string]TableDesc {
	out := make(map[string]TableDesc, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
