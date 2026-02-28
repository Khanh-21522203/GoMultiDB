package catalog

import (
	"context"
	"fmt"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
)

type TableState int

const (
	TablePreparing TableState = iota
	TableRunning
	TableAltering
	TableDeleting
	TableDeleted
)

type TableInfo struct {
	TableID     ids.TableID
	NamespaceID string
	Name        string
	State       TableState
	Version     uint64
	Epoch       uint64
	CreateReqID ids.RequestID
}

type CreateTableRequest struct {
	RequestID   ids.RequestID
	NamespaceID string
	Name        string
}

type CatalogSnapshot struct {
	Tables        map[ids.TableID]TableInfo
	tableNameToID map[string]ids.TableID
}

type CatalogMutation struct {
	RequestID   ids.RequestID
	UpsertTable []TableInfo
}

type CatalogStore interface {
	Apply(ctx context.Context, m CatalogMutation) error
	LoadSnapshot(ctx context.Context) (*CatalogSnapshot, error)
}

type MemoryStore struct {
	mu     sync.Mutex
	tables map[ids.TableID]TableInfo
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{tables: make(map[ids.TableID]TableInfo)}
}

func (s *MemoryStore) Apply(_ context.Context, m CatalogMutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range m.UpsertTable {
		s.tables[t.TableID] = t
	}
	return nil
}

func (s *MemoryStore) LoadSnapshot(_ context.Context) (*CatalogSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tables := make(map[ids.TableID]TableInfo, len(s.tables))
	nameIndex := make(map[string]ids.TableID, len(s.tables))
	for id, t := range s.tables {
		tables[id] = t
		nameIndex[tableNameKey(t.NamespaceID, t.Name)] = id
	}
	return &CatalogSnapshot{Tables: tables, tableNameToID: nameIndex}, nil
}

type createTableResult struct {
	fingerprint string
	tableID     ids.TableID
}

type TabletReportDelta struct {
	TSUUID        string
	IsIncremental bool
	SequenceNo    uint64
	Updated       []string
	RemovedIDs    []string
}

type ReconcileSink interface {
	ApplyTabletReport(ctx context.Context, delta TabletReportDelta) error
}

type noopReconcileSink struct{}

func (noopReconcileSink) ApplyTabletReport(_ context.Context, _ TabletReportDelta) error { return nil }

type Manager struct {
	mu       sync.RWMutex
	store    CatalogStore
	isLeader bool
	snap     *CatalogSnapshot
	dedupe   map[ids.RequestID]createTableResult
	sink     ReconcileSink
	memSink  *MemoryReconcileSink
}

func NewManager(store CatalogStore) (*Manager, error) {
	if store == nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "catalog store is required", false, nil)
	}
	snap, err := store.LoadSnapshot(context.Background())
	if err != nil {
		return nil, err
	}
	if snap == nil {
		snap = &CatalogSnapshot{Tables: make(map[ids.TableID]TableInfo), tableNameToID: make(map[string]ids.TableID)}
	}
	memSink := NewMemoryReconcileSink()
	return &Manager{
		store:   store,
		snap:    snap,
		dedupe:  make(map[ids.RequestID]createTableResult),
		sink:    memSink,
		memSink: memSink,
	}, nil
}

func (m *Manager) SetLeader(isLeader bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isLeader = isLeader
}

func (m *Manager) SetReconcileSink(sink ReconcileSink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if sink == nil {
		m.sink = noopReconcileSink{}
		m.memSink = nil
		return
	}
	m.sink = sink
	if ms, ok := sink.(*MemoryReconcileSink); ok {
		m.memSink = ms
	} else {
		m.memSink = nil
	}
}

func (m *Manager) GetMemoryReconcileSink() *MemoryReconcileSink {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.memSink
}

func (m *Manager) CreateTable(ctx context.Context, req CreateTableRequest) (ids.TableID, error) {
	if req.RequestID == "" || req.NamespaceID == "" || req.Name == "" {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "request id, namespace id and table name are required", false, nil)
	}

	fingerprint := req.NamespaceID + "/" + req.Name

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isLeader {
		return "", dberrors.New(dberrors.ErrNotLeader, "catalog mutation requires leader", true, nil)
	}
	if seen, ok := m.dedupe[req.RequestID]; ok {
		if seen.fingerprint != fingerprint {
			return "", dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different table create request", false, nil)
		}
		return seen.tableID, nil
	}
	if existingID, exists := m.snap.tableNameToID[tableNameKey(req.NamespaceID, req.Name)]; exists {
		return "", dberrors.New(dberrors.ErrConflict, fmt.Sprintf("table already exists: %s", existingID), false, nil)
	}

	rawID, err := ids.NewRequestID()
	if err != nil {
		return "", dberrors.New(dberrors.ErrInternal, "generate table id", false, err)
	}
	tableID := ids.TableID("table-" + string(rawID))

	table := TableInfo{
		TableID:     tableID,
		NamespaceID: req.NamespaceID,
		Name:        req.Name,
		State:       TablePreparing,
		Version:     1,
		Epoch:       1,
		CreateReqID: req.RequestID,
	}
	if err := transitionTableState(&table, TableRunning); err != nil {
		return "", err
	}

	if err := m.store.Apply(ctx, CatalogMutation{RequestID: req.RequestID, UpsertTable: []TableInfo{table}}); err != nil {
		return "", err
	}
	newSnap, err := m.store.LoadSnapshot(ctx)
	if err != nil {
		return "", err
	}
	m.snap = newSnap
	m.dedupe[req.RequestID] = createTableResult{fingerprint: fingerprint, tableID: tableID}
	return tableID, nil
}

func (m *Manager) GetTable(_ context.Context, tableID ids.TableID) (*TableInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.snap.Tables[tableID]
	if !ok {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "table not found", false, nil)
	}
	cp := t
	return &cp, nil
}

func (m *Manager) ApplyTabletReport(ctx context.Context, delta TabletReportDelta) error {
	if delta.TSUUID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "tablet report ts uuid is required", false, nil)
	}

	m.mu.RLock()
	isLeader := m.isLeader
	sink := m.sink
	m.mu.RUnlock()

	if !isLeader {
		return dberrors.New(dberrors.ErrNotLeader, "tablet report reconciliation requires leader", true, nil)
	}
	return sink.ApplyTabletReport(ctx, delta)
}

func transitionTableState(t *TableInfo, to TableState) error {
	if !isTableTransitionAllowed(t.State, to) {
		return dberrors.New(dberrors.ErrConflict, "invalid table state transition", true, nil)
	}
	t.State = to
	t.Version++
	return nil
}

func isTableTransitionAllowed(from, to TableState) bool {
	if from == to {
		return true
	}
	switch from {
	case TablePreparing:
		return to == TableRunning
	case TableRunning:
		return to == TableAltering || to == TableDeleting
	case TableAltering:
		return to == TableRunning || to == TableDeleting
	case TableDeleting:
		return to == TableDeleted
	case TableDeleted:
		return false
	default:
		return false
	}
}

func tableNameKey(namespaceID, name string) string {
	return namespaceID + "\x00" + name
}
