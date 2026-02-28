package tablet

import (
	"context"
	"fmt"
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

const AnyStateVersion uint64 = 0

type Meta struct {
	TabletID      string
	TableID       string
	Partition     partition.PartitionBound
	SplitParentID string
	SplitDepth    uint32
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
}

func NewManager() *Manager {
	return &Manager{
		peers: make(map[string]*Peer),
		ops:   make(map[string]bool),
	}
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
	meta.StateVersion++
	peer := &Peer{Meta: meta, State: Running}
	m.peers[meta.TabletID] = peer
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
		if err := transitionPeerState(p, Tombstoned); err != nil {
			return err
		}
		return nil
	}
	if err := transitionPeerState(p, Deleting); err != nil {
		return err
	}
	if err := transitionPeerState(p, Deleted); err != nil {
		return err
	}
	delete(m.peers, tabletID)
	return nil
}

func (m *Manager) SplitTablet(ctx context.Context, tabletID string, splitKey []byte, reqID string, pmap *partition.Map) (leftID, rightID string, err error) {
	return m.SplitTabletWithExpectedStateVersion(ctx, tabletID, splitKey, reqID, pmap, AnyStateVersion)
}

func (m *Manager) SplitTabletWithExpectedStateVersion(ctx context.Context, tabletID string, splitKey []byte, reqID string, pmap *partition.Map, expectedStateVersion uint64) (leftID, rightID string, err error) {
	if len(splitKey) == 0 {
		return "", "", dberrors.New(dberrors.ErrInvalidArgument, "split key is required", false, nil)
	}
	if !m.beginOp(tabletID) {
		return "", "", dberrors.New(dberrors.ErrConflict, "tablet operation in progress", true, nil)
	}
	defer m.endOp(tabletID)

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
	if len(parent.Meta.Partition.EndKey) > 0 && string(splitKey) >= string(parent.Meta.Partition.EndKey) {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrInvalidArgument, "split key outside parent range", false, nil)
	}
	if string(splitKey) <= string(parent.Meta.Partition.StartKey) {
		m.mu.Unlock()
		return "", "", dberrors.New(dberrors.ErrInvalidArgument, "split key outside parent range", false, nil)
	}
	if err := transitionPeerState(parent, Splitting); err != nil {
		m.mu.Unlock()
		return "", "", err
	}
	parentSplitStateVersion := parent.Meta.StateVersion

	leftID = fmt.Sprintf("%s-L", tabletID)
	rightID = fmt.Sprintf("%s-R", tabletID)
	leftMeta := Meta{
		TabletID:      leftID,
		TableID:       parent.Meta.TableID,
		Partition:     partition.PartitionBound{StartKey: parent.Meta.Partition.StartKey, EndKey: clone(splitKey)},
		SplitParentID: tabletID,
		SplitDepth:    parent.Meta.SplitDepth + 1,
		State:         Running,
		StateVersion:  1,
	}
	rightMeta := Meta{
		TabletID:      rightID,
		TableID:       parent.Meta.TableID,
		Partition:     partition.PartitionBound{StartKey: clone(splitKey), EndKey: parent.Meta.Partition.EndKey},
		SplitParentID: tabletID,
		SplitDepth:    parent.Meta.SplitDepth + 1,
		State:         Running,
		StateVersion:  1,
	}
	m.peers[leftID] = &Peer{Meta: leftMeta, State: Running}
	m.peers[rightID] = &Peer{Meta: rightMeta, State: Running}
	m.mu.Unlock()

	if pmap != nil {
		if err := pmap.RegisterTabletSplit(
			tabletID,
			partition.TabletPartition{TabletID: leftID, Bound: leftMeta.Partition},
			partition.TabletPartition{TabletID: rightID, Bound: rightMeta.Partition},
		); err != nil {
			m.mu.Lock()
			delete(m.peers, leftID)
			delete(m.peers, rightID)
			if current, ok := m.peers[tabletID]; ok && current.Meta.StateVersion == parentSplitStateVersion {
				_ = transitionPeerState(current, Running)
			}
			m.mu.Unlock()
			return "", "", err
		}
	}

	m.mu.Lock()
	if current, ok := m.peers[tabletID]; ok {
		if current.Meta.StateVersion != parentSplitStateVersion {
			m.mu.Unlock()
			return "", "", dberrors.New(dberrors.ErrConflict, "tablet state version changed during split finalization", true, nil)
		}
		if err := transitionPeerState(current, Tombstoned); err != nil {
			m.mu.Unlock()
			return "", "", err
		}
	}
	m.mu.Unlock()
	_ = ctx
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
	if p.State == Failed {
		if err := transitionPeerState(p, RemoteBootstrapping); err != nil {
			return err
		}
	}
	if err := transitionPeerState(p, Running); err != nil {
		return err
	}
	p.LastError = ""
	return nil
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

func transitionPeerState(p *Peer, to State) error {
	if p == nil {
		return dberrors.New(dberrors.ErrInvalidArgument, "peer is required", false, nil)
	}
	if !isTransitionAllowed(p.State, to) {
		return dberrors.New(dberrors.ErrConflict, "invalid tablet state transition", true, nil)
	}
	p.State = to
	p.Meta.State = to
	p.Meta.StateVersion++
	return nil
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
