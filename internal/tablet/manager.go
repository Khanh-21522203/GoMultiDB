package tablet

import (
	"context"
	"fmt"
	"strings"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/partition"
)

type State int

const (
	NotStarted State = iota
	Bootstrapping
	Running
	Splitting
	Tombstoned
	RemoteBootstrapping
	Deleting
	Deleted
	Failed
)

func (s State) String() string {
	switch s {
	case NotStarted:
		return "NOT_STARTED"
	case Bootstrapping:
		return "BOOTSTRAPPING"
	case Running:
		return "RUNNING"
	case Splitting:
		return "SPLITTING"
	case Tombstoned:
		return "TOMBSTONED"
	case RemoteBootstrapping:
		return "REMOTE_BOOTSTRAPPING"
	case Deleting:
		return "DELETING"
	case Deleted:
		return "DELETED"
	case Failed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

const AnyStateVersion uint64 = 0
const AnyOwnerEpoch uint64 = 0

type TransferState string

const (
	TransferStateNone     TransferState = "NONE"
	TransferStatePrepared TransferState = "TRANSFER_PREPARED"
)

type Meta struct {
	TabletID      string
	TableID       string
	Partition     partition.PartitionBound
	SplitParentID string
	SplitDepth    uint32
	OwnerTSUUID   string
	OwnerEpoch    uint64
	TransferState TransferState
	TransferEpoch uint64
	PendingOwner  string
	State         State
	StateVersion  uint64
}

type Peer struct {
	Meta      Meta
	State     State
	LastError string
}

type Manager struct {
	mu    sync.RWMutex
	peers map[string]*Peer
	ops   map[string]bool
	store MetaStore
}

// NewManager returns an in-memory-only Manager (no filesystem durability).
// Use this for tests or single-process ephemeral workloads.
func NewManager() *Manager {
	return &Manager{
		peers: make(map[string]*Peer),
		ops:   make(map[string]bool),
		store: NoopMetaStore{},
	}
}

// NewManagerWithFS returns a Manager backed by a FileMetaStore rooted at metaDir.
//
// On startup it scans metaDir for existing *.meta files and applies recovery rules:
//   - Running / Tombstoned / Failed  → restored as-is.
//   - Deleting                       → deletion is completed; tablet removed from registry.
//   - Bootstrapping / Splitting / RemoteBootstrapping / NotStarted → treated as Failed.
//   - Deleted                        → stale file removed; tablet not added to registry.
func NewManagerWithFS(metaDir string) (*Manager, error) {
	store, err := NewFileMetaStore(metaDir)
	if err != nil {
		return nil, err
	}
	metas, err := store.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("tablet manager: load metas: %w", err)
	}

	peers := make(map[string]*Peer, len(metas))
	for _, meta := range metas {
		normalizeOwnershipMeta(&meta)
		if meta.TransferState == TransferStatePrepared {
			// Transfer prepare is not durable across process crashes without a
			// consensus log; require master to issue a fresh prepare.
			meta.TransferState = TransferStateNone
			meta.TransferEpoch = 0
			meta.PendingOwner = ""
			meta.StateVersion++
			_ = store.WriteMeta(meta) // best-effort on recovery
		}
		switch meta.State {
		case Deleted:
			// Stale file; the tablet was already removed. Clean up.
			_ = store.DeleteMeta(meta.TabletID)

		case Deleting:
			// The process crashed between writing Deleting and completing deletion.
			// Complete the deletion now.
			_ = store.DeleteMeta(meta.TabletID)

		case NotStarted, Bootstrapping, Splitting, RemoteBootstrapping:
			// Incomplete operation. Promote to Failed so the operator or master can
			// decide whether to retry or remote-bootstrap.
			original := meta.State
			meta.State = Failed
			meta.StateVersion++
			_ = store.WriteMeta(meta) // best-effort; tolerate write error on recovery
			peers[meta.TabletID] = &Peer{
				Meta:      meta,
				State:     Failed,
				LastError: fmt.Sprintf("recovered from incomplete state: %s", original),
			}

		case Running, Tombstoned, Failed:
			p := meta // copy
			peers[meta.TabletID] = &Peer{Meta: p, State: p.State}
		}
	}

	return &Manager{
		peers: peers,
		ops:   make(map[string]bool),
		store: store,
	}, nil
}

// nextMeta validates the transition from p.State → to and returns the new Meta
// without mutating p. The caller must call applyMeta after a successful disk write.
func nextMeta(p *Peer, to State) (Meta, error) {
	if !isTransitionAllowed(p.State, to) {
		return Meta{}, dberrors.New(dberrors.ErrConflict,
			fmt.Sprintf("invalid tablet state transition: %s -> %s", p.State, to), true, nil)
	}
	m := p.Meta
	m.State = to
	m.StateVersion++
	return m, nil
}

// applyMeta updates p with the already-persisted meta. Must be called after
// a successful store.WriteMeta call for the same meta value.
func applyMeta(p *Peer, m Meta) {
	p.Meta = m
	p.State = m.State
}

func (m *Manager) CreateTablet(_ context.Context, meta Meta, _ string) error {
	if meta.TabletID == "" || meta.TableID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "tablet id and table id are required", false, nil)
	}
	if !m.beginOp(meta.TabletID) {
		return dberrors.New(dberrors.ErrConflict, "tablet operation in progress", true, nil)
	}
	defer m.endOp(meta.TabletID)

	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.peers[meta.TabletID]; ok {
		if equivalentMeta(existing.Meta, meta) {
			return nil
		}
		return dberrors.New(dberrors.ErrIdempotencyConflict, "tablet already exists with different metadata", false, nil)
	}

	meta.State = Running
	meta.StateVersion = 1
	normalizeOwnershipMeta(&meta)
	meta.TransferState = TransferStateNone
	meta.TransferEpoch = 0
	meta.PendingOwner = ""

	// Write to disk before making the peer visible to the registry.
	if err := m.store.WriteMeta(meta); err != nil {
		return fmt.Errorf("tablet manager: persist create %q: %w", meta.TabletID, err)
	}
	m.peers[meta.TabletID] = &Peer{Meta: meta, State: Running}
	return nil
}

func (m *Manager) OpenTablet(_ context.Context, tabletID string) (*Peer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[tabletID]
	if !ok {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "tablet not found", false, nil)
	}
	cp := *p
	return &cp, nil
}

func (m *Manager) DeleteTablet(ctx context.Context, tabletID string, tombstone bool, reqID string) error {
	return m.DeleteTabletWithExpectedStateVersion(ctx, tabletID, tombstone, reqID, AnyStateVersion)
}

func (m *Manager) DeleteTabletWithExpectedStateVersion(_ context.Context, tabletID string, tombstone bool, _ string, expectedStateVersion uint64) error {
	if !m.beginOp(tabletID) {
		return dberrors.New(dberrors.ErrConflict, "tablet operation in progress", true, nil)
	}
	defer m.endOp(tabletID)

	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[tabletID]
	if !ok {
		return nil
	}
	if err := ensureExpectedStateVersion(p, expectedStateVersion); err != nil {
		return err
	}

	if tombstone {
		newMeta, err := nextMeta(p, Tombstoned)
		if err != nil {
			return err
		}
		if err := m.store.WriteMeta(newMeta); err != nil {
			return fmt.Errorf("tablet manager: persist tombstone %q: %w", tabletID, err)
		}
		applyMeta(p, newMeta)
		return nil
	}

	// Hard delete: Deleting → Deleted → remove file → remove from registry.
	deletingMeta, err := nextMeta(p, Deleting)
	if err != nil {
		return err
	}
	if err := m.store.WriteMeta(deletingMeta); err != nil {
		return fmt.Errorf("tablet manager: persist deleting %q: %w", tabletID, err)
	}
	applyMeta(p, deletingMeta)

	// Remove the file before the in-memory deletion so a crash here still
	// results in a Deleting marker (which recovery will complete).
	if err := m.store.DeleteMeta(tabletID); err != nil {
		return fmt.Errorf("tablet manager: delete meta file %q: %w", tabletID, err)
	}
	// Transition in-memory only (file is gone).
	p.State = Deleted
	p.Meta.State = Deleted
	p.Meta.StateVersion++
	delete(m.peers, tabletID)
	return nil
}

func (m *Manager) SplitTablet(ctx context.Context, tabletID string, splitKey []byte, reqID string, pmap *partition.Map) (leftID, rightID string, err error) {
	return m.SplitTabletWithExpectedStateVersion(ctx, tabletID, splitKey, reqID, pmap, AnyStateVersion)
}

func (m *Manager) SplitTabletWithExpectedStateVersion(_ context.Context, tabletID string, splitKey []byte, reqID string, pmap *partition.Map, expectedStateVersion uint64) (leftID, rightID string, err error) {
	if len(splitKey) == 0 {
		return "", "", dberrors.New(dberrors.ErrInvalidArgument, "split key is required", false, nil)
	}
	if !m.beginOp(tabletID) {
		return "", "", dberrors.New(dberrors.ErrConflict, "tablet operation in progress", true, nil)
	}
	defer m.endOp(tabletID)

	// ── Phase 1: validate and compute new metas under lock ──────────────────

	m.mu.Lock()
	parent, ok := m.peers[tabletID]
	if !ok {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrInvalidArgument, "tablet not found", false, nil)
	}
	if err := ensureExpectedStateVersion(parent, expectedStateVersion); err != nil {
		m.mu.Unlock()
		return "", "", err
	}
	if parent.State != Running {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrConflict, "tablet not in running state", true, nil)
	}
	if parent.Meta.TransferState == TransferStatePrepared {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrConflict, "tablet ownership transfer is in prepared state", true, nil)
	}
	if len(parent.Meta.Partition.EndKey) > 0 && string(splitKey) >= string(parent.Meta.Partition.EndKey) {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrInvalidArgument, "split key outside parent range", false, nil)
	}
	if string(splitKey) <= string(parent.Meta.Partition.StartKey) {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrInvalidArgument, "split key outside parent range", false, nil)
	}

	parentSplitMeta, err := nextMeta(parent, Splitting)
	if err != nil {
		m.mu.Unlock()
		return "", "", err
	}

	leftID = fmt.Sprintf("%s-L", tabletID)
	rightID = fmt.Sprintf("%s-R", tabletID)
	leftMeta := Meta{
		TabletID:      leftID,
		TableID:       parent.Meta.TableID,
		Partition:     partition.PartitionBound{StartKey: clone(parent.Meta.Partition.StartKey), EndKey: clone(splitKey)},
		SplitParentID: tabletID,
		SplitDepth:    parent.Meta.SplitDepth + 1,
		OwnerTSUUID:   parent.Meta.OwnerTSUUID,
		OwnerEpoch:    parent.Meta.OwnerEpoch,
		TransferState: TransferStateNone,
		State:         Running,
		StateVersion:  1,
	}
	rightMeta := Meta{
		TabletID:      rightID,
		TableID:       parent.Meta.TableID,
		Partition:     partition.PartitionBound{StartKey: clone(splitKey), EndKey: clone(parent.Meta.Partition.EndKey)},
		SplitParentID: tabletID,
		SplitDepth:    parent.Meta.SplitDepth + 1,
		OwnerTSUUID:   parent.Meta.OwnerTSUUID,
		OwnerEpoch:    parent.Meta.OwnerEpoch,
		TransferState: TransferStateNone,
		State:         Running,
		StateVersion:  1,
	}

	// ── Phase 2: persist to disk before applying to in-memory state ─────────
	// Order: parent-splitting → children → (partition map) → parent-tombstone.
	if err := m.store.WriteMeta(parentSplitMeta); err != nil {
		m.mu.Unlock()
		return "", "", fmt.Errorf("tablet manager: persist split parent %q: %w", tabletID, err)
	}
	if err := m.store.WriteMeta(leftMeta); err != nil {
		m.mu.Unlock()
		return "", "", fmt.Errorf("tablet manager: persist left child %q: %w", leftID, err)
	}
	if err := m.store.WriteMeta(rightMeta); err != nil {
		m.mu.Unlock()
		return "", "", fmt.Errorf("tablet manager: persist right child %q: %w", rightID, err)
	}

	// ── Phase 3: apply in-memory ─────────────────────────────────────────────
	applyMeta(parent, parentSplitMeta)
	parentSplitStateVersion := parent.Meta.StateVersion
	m.peers[leftID] = &Peer{Meta: leftMeta, State: Running}
	m.peers[rightID] = &Peer{Meta: rightMeta, State: Running}
	m.mu.Unlock()

	// ── Phase 4: register split in partition map (outside lock) ──────────────
	if pmap != nil {
		if err := pmap.RegisterTabletSplit(
			tabletID,
			partition.TabletPartition{TabletID: leftID, Bound: leftMeta.Partition},
			partition.TabletPartition{TabletID: rightID, Bound: rightMeta.Partition},
		); err != nil {
			// Rollback: remove children from disk and in-memory; revert parent to Running.
			_ = m.store.DeleteMeta(leftID)
			_ = m.store.DeleteMeta(rightID)

			m.mu.Lock()
			delete(m.peers, leftID)
			delete(m.peers, rightID)
			if current, ok := m.peers[tabletID]; ok && current.Meta.StateVersion == parentSplitStateVersion {
				revertMeta, rerr := nextMeta(current, Running)
				if rerr == nil {
					_ = m.store.WriteMeta(revertMeta) // best-effort
					applyMeta(current, revertMeta)
				}
			}
			m.mu.Unlock()
			return "", "", err
		}
	}

	// ── Phase 5: tombstone parent ────────────────────────────────────────────
	m.mu.Lock()
	current, ok := m.peers[tabletID]
	if !ok {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrConflict, "parent tablet disappeared during split", true, nil)
	}
	if current.Meta.StateVersion != parentSplitStateVersion {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrConflict, "tablet state version changed during split finalization", true, nil)
	}
	tombstoneMeta, err := nextMeta(current, Tombstoned)
	if err != nil {
		m.mu.Unlock()
		return "", "", err
	}
	if err := m.store.WriteMeta(tombstoneMeta); err != nil {
		m.mu.Unlock()
		return "", "", fmt.Errorf("tablet manager: persist tombstone parent %q: %w", tabletID, err)
	}
	applyMeta(current, tombstoneMeta)
	m.mu.Unlock()

	_ = reqID
	return leftID, rightID, nil
}

func (m *Manager) RemoteBootstrapTablet(ctx context.Context, tabletID string, sourcePeer string) error {
	return m.RemoteBootstrapTabletWithExpectedStateVersion(ctx, tabletID, sourcePeer, AnyStateVersion)
}

func (m *Manager) RemoteBootstrapTabletWithExpectedStateVersion(_ context.Context, tabletID string, sourcePeer string, expectedStateVersion uint64) error {
	if sourcePeer == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "source peer required", false, nil)
	}
	if !m.beginOp(tabletID) {
		return dberrors.New(dberrors.ErrConflict, "tablet operation in progress", true, nil)
	}
	defer m.endOp(tabletID)

	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[tabletID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "tablet not found", false, nil)
	}
	if err := ensureExpectedStateVersion(p, expectedStateVersion); err != nil {
		return err
	}
	if p.State != Tombstoned && p.State != Failed {
		return dberrors.New(dberrors.ErrConflict, "remote bootstrap requires tombstoned or failed tablet", true, nil)
	}
	if p.State == Tombstoned && hasActiveSplitChildren(m.peers, tabletID) {
		return dberrors.New(dberrors.ErrConflict, "cannot remote bootstrap split parent with active children", true, nil)
	}

	// Failed → RemoteBootstrapping → Running (two transitions, two writes).
	// Tombstoned → Running (one transition, one write).
	if p.State == Failed {
		rbMeta, err := nextMeta(p, RemoteBootstrapping)
		if err != nil {
			return err
		}
		if err := m.store.WriteMeta(rbMeta); err != nil {
			return fmt.Errorf("tablet manager: persist remote-bootstrapping %q: %w", tabletID, err)
		}
		applyMeta(p, rbMeta)
	}

	runMeta, err := nextMeta(p, Running)
	if err != nil {
		return err
	}
	if strings.TrimSpace(runMeta.OwnerTSUUID) == "" {
		runMeta.OwnerTSUUID = sourcePeer
	}
	if sourcePeer != "" && runMeta.OwnerTSUUID != sourcePeer {
		runMeta.OwnerTSUUID = sourcePeer
		runMeta.OwnerEpoch++
	}
	if runMeta.OwnerEpoch == 0 {
		runMeta.OwnerEpoch = 1
	}
	runMeta.TransferState = TransferStateNone
	runMeta.TransferEpoch = 0
	runMeta.PendingOwner = ""
	if err := m.store.WriteMeta(runMeta); err != nil {
		return fmt.Errorf("tablet manager: persist running (after remote-bootstrap) %q: %w", tabletID, err)
	}
	applyMeta(p, runMeta)
	p.LastError = ""
	return nil
}

func (m *Manager) TransferPrepare(_ context.Context, tabletID, fromOwner, toOwner string, expectedOwnerEpoch, expectedStateVersion uint64) (Meta, error) {
	if strings.TrimSpace(toOwner) == "" {
		return Meta{}, dberrors.New(dberrors.ErrInvalidArgument, "target owner is required", false, nil)
	}
	if !m.beginOp(tabletID) {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "tablet operation in progress", true, nil)
	}
	defer m.endOp(tabletID)

	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[tabletID]
	if !ok {
		return Meta{}, dberrors.New(dberrors.ErrInvalidArgument, "tablet not found", false, nil)
	}
	if p.State != Running {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "transfer prepare requires running tablet", true, nil)
	}
	if err := ensureExpectedStateVersion(p, expectedStateVersion); err != nil {
		return Meta{}, err
	}
	if err := ensureExpectedOwnerEpoch(p, expectedOwnerEpoch); err != nil {
		return Meta{}, err
	}
	if strings.TrimSpace(fromOwner) != "" && p.Meta.OwnerTSUUID != fromOwner {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "transfer source owner mismatch", true, nil)
	}
	if p.Meta.OwnerTSUUID == toOwner {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "tablet already owned by target owner", true, nil)
	}
	if p.Meta.TransferState == TransferStatePrepared {
		if p.Meta.PendingOwner == toOwner {
			return p.Meta, nil
		}
		return Meta{}, dberrors.New(dberrors.ErrConflict, "tablet transfer already prepared for different target", true, nil)
	}

	next := p.Meta
	next.TransferState = TransferStatePrepared
	next.PendingOwner = toOwner
	next.TransferEpoch = p.Meta.OwnerEpoch + 1
	next.StateVersion++
	if err := m.store.WriteMeta(next); err != nil {
		return Meta{}, fmt.Errorf("tablet manager: persist transfer prepare %q: %w", tabletID, err)
	}
	applyMeta(p, next)
	return next, nil
}

func (m *Manager) TransferCommit(_ context.Context, tabletID string, expectedTransferEpoch, expectedStateVersion uint64) (Meta, error) {
	if !m.beginOp(tabletID) {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "tablet operation in progress", true, nil)
	}
	defer m.endOp(tabletID)

	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.peers[tabletID]
	if !ok {
		return Meta{}, dberrors.New(dberrors.ErrInvalidArgument, "tablet not found", false, nil)
	}
	if p.State != Running {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "transfer commit requires running tablet", true, nil)
	}
	if err := ensureExpectedStateVersion(p, expectedStateVersion); err != nil {
		return Meta{}, err
	}
	if p.Meta.TransferState != TransferStatePrepared || strings.TrimSpace(p.Meta.PendingOwner) == "" {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "tablet transfer is not prepared", true, nil)
	}
	if expectedTransferEpoch != AnyOwnerEpoch && p.Meta.TransferEpoch != expectedTransferEpoch {
		return Meta{}, dberrors.New(dberrors.ErrConflict, "tablet transfer epoch precondition mismatch", true, nil)
	}

	next := p.Meta
	next.OwnerTSUUID = p.Meta.PendingOwner
	if p.Meta.TransferEpoch > 0 {
		next.OwnerEpoch = p.Meta.TransferEpoch
	} else {
		next.OwnerEpoch++
	}
	next.TransferState = TransferStateNone
	next.TransferEpoch = 0
	next.PendingOwner = ""
	next.StateVersion++
	if err := m.store.WriteMeta(next); err != nil {
		return Meta{}, fmt.Errorf("tablet manager: persist transfer commit %q: %w", tabletID, err)
	}
	applyMeta(p, next)
	return next, nil
}

func (m *Manager) ListTablets(_ context.Context) []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Peer, 0, len(m.peers))
	for _, p := range m.peers {
		cp := *p
		out = append(out, cp)
	}
	return out
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (m *Manager) beginOp(tabletID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ops[tabletID] {
		return false
	}
	m.ops[tabletID] = true
	return true
}

func (m *Manager) endOp(tabletID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ops, tabletID)
}

func equivalentMeta(a, b Meta) bool {
	return a.TabletID == b.TabletID && a.TableID == b.TableID &&
		string(a.Partition.StartKey) == string(b.Partition.StartKey) &&
		string(a.Partition.EndKey) == string(b.Partition.EndKey)
}

func ensureExpectedStateVersion(p *Peer, expectedStateVersion uint64) error {
	if expectedStateVersion == AnyStateVersion {
		return nil
	}
	if p.Meta.StateVersion != expectedStateVersion {
		return dberrors.New(dberrors.ErrConflict, "tablet state version precondition mismatch", true, nil)
	}
	return nil
}

func ensureExpectedOwnerEpoch(p *Peer, expectedOwnerEpoch uint64) error {
	if expectedOwnerEpoch == AnyOwnerEpoch {
		return nil
	}
	if p.Meta.OwnerEpoch != expectedOwnerEpoch {
		return dberrors.New(dberrors.ErrConflict, "tablet owner epoch precondition mismatch", true, nil)
	}
	return nil
}

func normalizeOwnershipMeta(meta *Meta) {
	if strings.TrimSpace(meta.OwnerTSUUID) == "" {
		meta.OwnerTSUUID = "local"
	}
	if meta.OwnerEpoch == 0 {
		meta.OwnerEpoch = 1
	}
	if meta.TransferState == "" {
		meta.TransferState = TransferStateNone
	}
	if meta.TransferState != TransferStatePrepared {
		meta.TransferEpoch = 0
		meta.PendingOwner = ""
	}
}

func hasActiveSplitChildren(peers map[string]*Peer, parentID string) bool {
	for _, peer := range peers {
		if peer.Meta.SplitParentID != parentID {
			continue
		}
		if peer.State == Running || peer.State == Bootstrapping || peer.State == RemoteBootstrapping {
			return true
		}
	}
	return false
}

func isTransitionAllowed(from, to State) bool {
	if from == to {
		return true
	}
	switch from {
	case NotStarted:
		return to == Bootstrapping
	case Bootstrapping:
		return to == Running || to == Failed
	case Running:
		return to == Splitting || to == Tombstoned || to == Deleting || to == Failed
	case Splitting:
		return to == Tombstoned || to == Running || to == Failed
	case Tombstoned:
		return to == RemoteBootstrapping || to == Deleting || to == Running
	case RemoteBootstrapping:
		return to == Running || to == Failed
	case Deleting:
		return to == Deleted
	case Deleted:
		return false
	case Failed:
		return to == RemoteBootstrapping || to == Deleting
	default:
		return false
	}
}

func clone(v []byte) []byte {
	if v == nil {
		return nil
	}
	return append([]byte(nil), v...)
}
