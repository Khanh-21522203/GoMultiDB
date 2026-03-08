package catalog

import (
	"context"
	"fmt"
	"strings"
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
	Tablets       map[ids.TabletID]TabletInfo
}

// TabletState mirrors the tablet lifecycle states tracked in the master catalog.
type TabletState int

const (
	TabletPreparing TabletState = iota
	TabletCreating
	TabletRunning
	TabletTombstoned
	TabletDeleted
)

// TabletInfo is the catalog-side record for a tablet replica group.
type TabletInfo struct {
	TabletID    ids.TabletID
	TableID     ids.TableID
	NamespaceID string
	State       TabletState
	// ReplicaCount is the current number of confirmed replicas.
	ReplicaCount int
}

type CatalogMutation struct {
	RequestID          ids.RequestID
	RequestKind        string
	RequestFingerprint string
	RequestValue       string
	UpsertTable        []TableInfo
	UpsertTablet       []TabletInfo
}

// CatalogStore is the persistence interface for catalog state.
type CatalogStore interface {
	Apply(ctx context.Context, m CatalogMutation) error
	LoadSnapshot(ctx context.Context) (*CatalogSnapshot, error)
}

// NewSnapshot constructs a CatalogSnapshot from pre-populated maps.
// Used by SysCatalogStore.LoadSnapshot.
func NewSnapshot(
	tables map[ids.TableID]TableInfo,
	nameToID map[string]ids.TableID,
	tablets map[ids.TabletID]TabletInfo,
) *CatalogSnapshot {
	return &CatalogSnapshot{
		Tables:        tables,
		tableNameToID: nameToID,
		Tablets:       tablets,
	}
}

type MemoryStore struct {
	mu      sync.Mutex
	tables  map[ids.TableID]TableInfo
	tablets map[ids.TabletID]TabletInfo
	reqlog  map[ids.RequestID][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tables:  make(map[ids.TableID]TableInfo),
		tablets: make(map[ids.TabletID]TabletInfo),
		reqlog:  make(map[ids.RequestID][]byte),
	}
}

func (s *MemoryStore) Apply(_ context.Context, m CatalogMutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range m.UpsertTable {
		s.tables[t.TableID] = t
	}
	for _, ti := range m.UpsertTablet {
		s.tablets[ti.TabletID] = ti
	}
	if m.RequestID != "" {
		payload := []byte("1")
		if m.RequestKind != "" || m.RequestFingerprint != "" || m.RequestValue != "" {
			payload = []byte(m.RequestKind + "|" + m.RequestFingerprint + "|" + m.RequestValue)
		}
		s.reqlog[m.RequestID] = payload
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
	tablets := make(map[ids.TabletID]TabletInfo, len(s.tablets))
	for id, ti := range s.tablets {
		tablets[id] = ti
	}
	return &CatalogSnapshot{Tables: tables, tableNameToID: nameIndex, Tablets: tablets}, nil
}

type dedupeEntry struct {
	kind        string
	fingerprint string
	value       string
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
	dedupe   map[ids.RequestID]dedupeEntry
	sink     ReconcileSink
	memSink  *MemoryReconcileSink
	// lastSeq tracks the most recent sequence number seen per tserver UUID.
	// Used by ProcessTabletReport to drop stale/out-of-order updates.
	lastSeq map[string]uint64
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
		snap = &CatalogSnapshot{
			Tables:        make(map[ids.TableID]TableInfo),
			tableNameToID: make(map[string]ids.TableID),
			Tablets:       make(map[ids.TabletID]TabletInfo),
		}
	}
	if snap.Tablets == nil {
		snap.Tablets = make(map[ids.TabletID]TabletInfo)
	}
	memSink := NewMemoryReconcileSink()
	return &Manager{
		store:   store,
		snap:    snap,
		dedupe:  make(map[ids.RequestID]dedupeEntry),
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
		if seen.kind != "create_table" || seen.fingerprint != fingerprint {
			return "", dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different table create request", false, nil)
		}
		return ids.TableID(seen.value), nil
	}
	if seen, ok, err := m.lookupDurableDedupe(ctx, req.RequestID); err != nil {
		return "", err
	} else if ok {
		m.dedupe[req.RequestID] = seen
		if seen.kind != "create_table" || seen.fingerprint != fingerprint {
			return "", dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different table create request", false, nil)
		}
		return ids.TableID(seen.value), nil
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

	if err := m.store.Apply(ctx, CatalogMutation{
		RequestID:          req.RequestID,
		RequestKind:        "create_table",
		RequestFingerprint: fingerprint,
		RequestValue:       string(tableID),
		UpsertTable:        []TableInfo{table},
	}); err != nil {
		return "", err
	}
	newSnap, err := m.store.LoadSnapshot(ctx)
	if err != nil {
		return "", err
	}
	m.snap = newSnap
	m.dedupe[req.RequestID] = dedupeEntry{kind: "create_table", fingerprint: fingerprint, value: string(tableID)}
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

// GetTableByName returns the TableInfo for a table identified by namespace and name.
func (m *Manager) GetTableByName(_ context.Context, namespaceID, name string) (*TableInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.snap.tableNameToID[tableNameKey(namespaceID, name)]
	if !ok {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "table not found", false, nil)
	}
	t := m.snap.Tables[id]
	cp := t
	return &cp, nil
}

// AlterTableRequest carries the parameters for an AlterTable operation.
type AlterTableRequest struct {
	RequestID ids.RequestID
	TableID   ids.TableID
}

// AlterTable transitions the table through Running → Altering → Running and
// bumps the Version.  Idempotent by RequestID.
func (m *Manager) AlterTable(ctx context.Context, req AlterTableRequest) error {
	if req.RequestID == "" || req.TableID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "request id and table id are required", false, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isLeader {
		return dberrors.New(dberrors.ErrNotLeader, "catalog mutation requires leader", true, nil)
	}
	if seen, ok := m.dedupe[req.RequestID]; ok {
		if seen.kind != "alter_table" || seen.value != string(req.TableID) {
			return dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different alter table request", false, nil)
		}
		return nil
	}
	if seen, ok, err := m.lookupDurableDedupe(ctx, req.RequestID); err != nil {
		return err
	} else if ok {
		m.dedupe[req.RequestID] = seen
		if seen.kind != "alter_table" || seen.value != string(req.TableID) {
			return dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different alter table request", false, nil)
		}
		return nil
	}
	t, ok := m.snap.Tables[req.TableID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "table not found", false, nil)
	}
	// Running → Altering → Running (two transitions, one persist).
	if err := transitionTableState(&t, TableAltering); err != nil {
		return err
	}
	if err := transitionTableState(&t, TableRunning); err != nil {
		return err
	}
	if err := m.store.Apply(ctx, CatalogMutation{
		RequestID:          req.RequestID,
		RequestKind:        "alter_table",
		RequestFingerprint: string(req.TableID),
		RequestValue:       string(req.TableID),
		UpsertTable:        []TableInfo{t},
	}); err != nil {
		return err
	}
	newSnap, err := m.store.LoadSnapshot(ctx)
	if err != nil {
		return err
	}
	m.snap = newSnap
	m.dedupe[req.RequestID] = dedupeEntry{kind: "alter_table", fingerprint: string(req.TableID), value: string(req.TableID)}
	return nil
}

// DeleteTableRequest carries the parameters for a DeleteTable operation.
type DeleteTableRequest struct {
	RequestID ids.RequestID
	TableID   ids.TableID
}

// DeleteTable transitions the table to Deleting and tombstones its tablets.
// Idempotent by RequestID.
func (m *Manager) DeleteTable(ctx context.Context, req DeleteTableRequest) error {
	if req.RequestID == "" || req.TableID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "request id and table id are required", false, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isLeader {
		return dberrors.New(dberrors.ErrNotLeader, "catalog mutation requires leader", true, nil)
	}
	if seen, ok := m.dedupe[req.RequestID]; ok {
		if seen.kind != "delete_table" || seen.value != string(req.TableID) {
			return dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different delete table request", false, nil)
		}
		return nil
	}
	if seen, ok, err := m.lookupDurableDedupe(ctx, req.RequestID); err != nil {
		return err
	} else if ok {
		m.dedupe[req.RequestID] = seen
		if seen.kind != "delete_table" || seen.value != string(req.TableID) {
			return dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different delete table request", false, nil)
		}
		return nil
	}
	t, ok := m.snap.Tables[req.TableID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "table not found", false, nil)
	}
	if err := transitionTableState(&t, TableDeleting); err != nil {
		return err
	}
	// Tombstone all non-terminal tablets belonging to this table.
	var updatedTablets []TabletInfo
	for _, ti := range m.snap.Tablets {
		if ti.TableID != req.TableID {
			continue
		}
		if ti.State == TabletTombstoned || ti.State == TabletDeleted {
			continue
		}
		ti.State = TabletTombstoned
		updatedTablets = append(updatedTablets, ti)
	}
	mut := CatalogMutation{
		RequestID:          req.RequestID,
		RequestKind:        "delete_table",
		RequestFingerprint: string(req.TableID),
		RequestValue:       string(req.TableID),
		UpsertTable:        []TableInfo{t},
		UpsertTablet:       updatedTablets,
	}
	if err := m.store.Apply(ctx, mut); err != nil {
		return err
	}
	newSnap, err := m.store.LoadSnapshot(ctx)
	if err != nil {
		return err
	}
	m.snap = newSnap
	m.dedupe[req.RequestID] = dedupeEntry{kind: "delete_table", fingerprint: string(req.TableID), value: string(req.TableID)}
	return nil
}

// CreateTablet registers a new tablet in the catalog.
func (m *Manager) CreateTablet(ctx context.Context, ti TabletInfo, reqID ids.RequestID) (ids.TabletID, error) {
	if reqID == "" || ti.TableID == "" {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "request id and table id are required", false, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isLeader {
		return "", dberrors.New(dberrors.ErrNotLeader, "catalog mutation requires leader", true, nil)
	}
	if seen, ok := m.dedupe[reqID]; ok {
		if seen.kind != "create_tablet" {
			return "", dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different create tablet request", false, nil)
		}
		return ids.TabletID(seen.value), nil
	}
	if seen, ok, err := m.lookupDurableDedupe(ctx, reqID); err != nil {
		return "", err
	} else if ok {
		m.dedupe[reqID] = seen
		if seen.kind != "create_tablet" {
			return "", dberrors.New(dberrors.ErrIdempotencyConflict, "request id already used with different create tablet request", false, nil)
		}
		return ids.TabletID(seen.value), nil
	}
	if ti.TabletID == "" {
		rawID, err := ids.NewRequestID()
		if err != nil {
			return "", dberrors.New(dberrors.ErrInternal, "generate tablet id", false, err)
		}
		ti.TabletID = ids.TabletID("tablet-" + string(rawID))
	}
	ti.State = TabletPreparing
	if err := m.store.Apply(ctx, CatalogMutation{
		RequestID:          reqID,
		RequestKind:        "create_tablet",
		RequestFingerprint: string(ti.TableID),
		RequestValue:       string(ti.TabletID),
		UpsertTablet:       []TabletInfo{ti},
	}); err != nil {
		return "", err
	}
	newSnap, err := m.store.LoadSnapshot(ctx)
	if err != nil {
		return "", err
	}
	m.snap = newSnap
	m.dedupe[reqID] = dedupeEntry{kind: "create_tablet", fingerprint: string(ti.TableID), value: string(ti.TabletID)}
	return ti.TabletID, nil
}

// TabletReport carries incremental or full tablet state from a tserver heartbeat.
type TabletReport struct {
	TSUUID        string
	IsIncremental bool
	SequenceNo    uint64
	Tablets       []TabletInfo
}

// lastSeenSeq tracks the most recently applied sequence number per (tsUUID, tabletID).
// This is held in Manager so it doesn't require persistence (reconstructed from heartbeats).
func (m *Manager) initSeqTracker() {
	if m.lastSeq == nil {
		m.lastSeq = make(map[string]uint64)
	}
}

// ProcessTabletReport applies an incremental or full tablet report from a tserver,
// enforcing sequence number monotonicity to drop stale/out-of-order updates.
func (m *Manager) ProcessTabletReport(ctx context.Context, report TabletReport) error {
	if report.TSUUID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "tserver uuid required", false, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isLeader {
		return dberrors.New(dberrors.ErrNotLeader, "tablet report requires leader", true, nil)
	}
	m.initSeqTracker()

	seqKey := report.TSUUID
	if !report.IsIncremental {
		// Full report resets the sequence number for this tserver.
		m.lastSeq[seqKey] = report.SequenceNo
	} else {
		if report.SequenceNo <= m.lastSeq[seqKey] {
			// Stale or duplicate — drop silently.
			return nil
		}
		m.lastSeq[seqKey] = report.SequenceNo
	}

	// Apply valid tablet deltas.
	if len(report.Tablets) == 0 {
		return nil
	}
	mut := CatalogMutation{UpsertTablet: report.Tablets}
	if err := m.store.Apply(ctx, mut); err != nil {
		return err
	}
	newSnap, err := m.store.LoadSnapshot(ctx)
	if err != nil {
		return err
	}
	m.snap = newSnap
	return nil
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

func (s *MemoryStore) SeenRequest(_ context.Context, reqID ids.RequestID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.reqlog[reqID]
	return ok, nil
}

func (s *MemoryStore) RequestValue(_ context.Context, reqID ids.RequestID) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.reqlog[reqID]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func tableNameKey(namespaceID, name string) string {
	return namespaceID + "\x00" + name
}

func parseDedupeValue(v []byte) (dedupeEntry, bool) {
	s := string(v)
	parts := strings.SplitN(s, "|", 3)
	if len(parts) != 3 {
		return dedupeEntry{}, false
	}
	return dedupeEntry{kind: parts[0], fingerprint: parts[1], value: parts[2]}, true
}

func (m *Manager) lookupDurableDedupe(ctx context.Context, reqID ids.RequestID) (dedupeEntry, bool, error) {
	ss, ok := m.store.(interface {
		SeenRequest(context.Context, ids.RequestID) (bool, error)
		RequestValue(context.Context, ids.RequestID) ([]byte, bool, error)
	})
	if !ok {
		return dedupeEntry{}, false, nil
	}
	seen, err := ss.SeenRequest(ctx, reqID)
	if err != nil {
		return dedupeEntry{}, false, err
	}
	if !seen {
		return dedupeEntry{}, false, nil
	}
	v, ok, err := ss.RequestValue(ctx, reqID)
	if err != nil {
		return dedupeEntry{}, false, err
	}
	if !ok {
		return dedupeEntry{}, false, nil
	}
	if ent, parsed := parseDedupeValue(v); parsed {
		return ent, true, nil
	}
	return dedupeEntry{}, false, nil
}
