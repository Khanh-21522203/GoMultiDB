// Package snapshot implements the distributed snapshot coordinator.
//
// Design (docs/plans/plan-snapshotting-and-backup.md):
//
//   - Coordinator owns a map of SnapshotID → SnapshotInfo.
//   - CreateSnapshot fans out tablet snapshot tasks, aggregates results.
//   - DeleteSnapshot fans out delete tasks.
//   - RestoreSnapshot fans out restore tasks and republishes catalog metadata.
//
// Persistence model: snapshot descriptors are stored in the catalog store
// (via UpsertSnapshot mutation). On restart, in-progress snapshots are
// reloaded and incomplete tasks are re-fanned-out.
package snapshot

import (
	"context"
	"fmt"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

// ── Data Structures ───────────────────────────────────────────────────────────

// State is the lifecycle state of a snapshot.
type State int

const (
	StateCreating  State = iota
	StateComplete
	StateFailed
	StateDeleting
	StateRestoring
)

func (s State) String() string {
	switch s {
	case StateCreating:
		return "CREATING"
	case StateComplete:
		return "COMPLETE"
	case StateFailed:
		return "FAILED"
	case StateDeleting:
		return "DELETING"
	case StateRestoring:
		return "RESTORING"
	default:
		return "UNKNOWN"
	}
}

// SnapshotInfo is the coordinator-side descriptor for one snapshot.
type SnapshotInfo struct {
	SnapshotID   string
	NamespaceIDs []string
	TableIDs     []string
	TabletIDs    []string
	CreateHT     uint64
	State        State
	CreatedAt    time.Time
	// Error is set when State == StateFailed.
	Error string
}

// TabletSnapshotTask tracks per-tablet task state for a snapshot.
type TabletSnapshotTask struct {
	SnapshotID string
	TabletID   string
	State      State
	Error      string
}

// ── Tablet RPC interface ──────────────────────────────────────────────────────

// TabletSnapshotRPC is the interface the coordinator uses to issue per-tablet
// snapshot operations. In production this is a gRPC stub; in tests it is mocked.
type TabletSnapshotRPC interface {
	CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error
	DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error
	RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error
}

// ── Coordinator ───────────────────────────────────────────────────────────────

// Config controls coordinator behaviour.
type SnapshotStore interface {
	SaveSnapshot(ctx context.Context, info SnapshotInfo) error
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	LoadSnapshots(ctx context.Context) ([]SnapshotInfo, error)
}

type memorySnapshotStore struct {
	mu    sync.Mutex
	items map[string]SnapshotInfo
}

func newMemorySnapshotStore() *memorySnapshotStore {
	return &memorySnapshotStore{items: make(map[string]SnapshotInfo)}
}

func (s *memorySnapshotStore) SaveSnapshot(_ context.Context, info SnapshotInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[info.SnapshotID] = info
	return nil
}

func (s *memorySnapshotStore) DeleteSnapshot(_ context.Context, snapshotID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, snapshotID)
	return nil
}

func (s *memorySnapshotStore) LoadSnapshots(_ context.Context) ([]SnapshotInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SnapshotInfo, 0, len(s.items))
	for _, v := range s.items {
		out = append(out, v)
	}
	return out, nil
}

// Config controls coordinator behaviour.
type Config struct {
	// MaxConcurrentSnapshots limits in-flight fan-out goroutines per snapshot.
	MaxConcurrentSnapshots int
	// NowFn returns the current time (injectable for tests).
	NowFn func() time.Time
	// Store persists snapshot descriptors for restart recovery.
	Store SnapshotStore
}

func (c *Config) defaults() {
	if c.MaxConcurrentSnapshots <= 0 {
		c.MaxConcurrentSnapshots = 2
	}
	if c.NowFn == nil {
		c.NowFn = func() time.Time { return time.Now().UTC() }
	}
	if c.Store == nil {
		c.Store = newMemorySnapshotStore()
	}
}

// Coordinator manages the lifecycle of distributed snapshots.
type Coordinator struct {
	mu        sync.Mutex
	snapshots map[string]*SnapshotInfo
	rpc       TabletSnapshotRPC
	cfg       Config
	store     SnapshotStore
}

// NewCoordinator creates a Coordinator.
func NewCoordinator(rpc TabletSnapshotRPC, cfg Config) *Coordinator {
	cfg.defaults()
	c := &Coordinator{
		snapshots: make(map[string]*SnapshotInfo),
		rpc:       rpc,
		cfg:       cfg,
		store:     cfg.Store,
	}
	_ = c.Recover(context.Background())
	return c
}

// CreateSnapshot creates a new snapshot for the given tablets.
// It fans out CreateTabletSnapshot RPCs and aggregates results.
// Returns the snapshotID on success.
func (c *Coordinator) CreateSnapshot(ctx context.Context, snapshotID string, tabletIDs []string, createHT uint64) (*SnapshotInfo, error) {
	if snapshotID == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "snapshot id required", false, nil)
	}
	if len(tabletIDs) == 0 {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "at least one tablet id required", false, nil)
	}

	info := &SnapshotInfo{
		SnapshotID: snapshotID,
		TabletIDs:  tabletIDs,
		CreateHT:   createHT,
		State:      StateCreating,
		CreatedAt:  c.cfg.NowFn(),
	}

	c.mu.Lock()
	if _, exists := c.snapshots[snapshotID]; exists {
		c.mu.Unlock()
		return nil, dberrors.New(dberrors.ErrConflict, "snapshot already exists", false, nil)
	}
	c.snapshots[snapshotID] = info
	c.mu.Unlock()
	if err := c.store.SaveSnapshot(ctx, *info); err != nil {
		return nil, err
	}

	// Fan out per-tablet tasks with bounded concurrency.
	if err := c.fanOut(ctx, tabletIDs, func(ctx context.Context, tabletID string) error {
		return c.rpc.CreateTabletSnapshot(ctx, snapshotID, tabletID)
	}); err != nil {
		c.mu.Lock()
		info.State = StateFailed
		info.Error = err.Error()
		failed := *info
		c.mu.Unlock()
		_ = c.store.SaveSnapshot(ctx, failed)
		return info, err
	}

	c.mu.Lock()
	info.State = StateComplete
	completed := *info
	c.mu.Unlock()
	if err := c.store.SaveSnapshot(ctx, completed); err != nil {
		return nil, err
	}
	return info, nil
}

// DeleteSnapshot transitions a Complete snapshot to Deleting and fans out
// delete tasks to all involved tablets.
func (c *Coordinator) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	c.mu.Lock()
	info, ok := c.snapshots[snapshotID]
	if !ok {
		c.mu.Unlock()
		return dberrors.New(dberrors.ErrInvalidArgument, "snapshot not found", false, nil)
	}
	if info.State != StateComplete {
		c.mu.Unlock()
		return dberrors.New(dberrors.ErrConflict,
			fmt.Sprintf("snapshot must be COMPLETE to delete, is %s", info.State), false, nil)
	}
	info.State = StateDeleting
	deleting := *info
	tabletIDs := append([]string(nil), info.TabletIDs...)
	c.mu.Unlock()
	if err := c.store.SaveSnapshot(ctx, deleting); err != nil {
		return err
	}

	if err := c.fanOut(ctx, tabletIDs, func(ctx context.Context, tabletID string) error {
		return c.rpc.DeleteTabletSnapshot(ctx, snapshotID, tabletID)
	}); err != nil {
		// Non-fatal: leave in Deleting for retry.
		return err
	}

	c.mu.Lock()
	delete(c.snapshots, snapshotID)
	c.mu.Unlock()
	if err := c.store.DeleteSnapshot(ctx, snapshotID); err != nil {
		return err
	}
	return nil
}

// RestoreSnapshot fans out restore RPCs to all tablets in the snapshot.
// The snapshot must be in Complete state.
func (c *Coordinator) RestoreSnapshot(ctx context.Context, snapshotID string) error {
	c.mu.Lock()
	info, ok := c.snapshots[snapshotID]
	if !ok {
		c.mu.Unlock()
		return dberrors.New(dberrors.ErrInvalidArgument, "snapshot not found", false, nil)
	}
	if info.State != StateComplete {
		c.mu.Unlock()
		return dberrors.New(dberrors.ErrConflict,
			fmt.Sprintf("snapshot must be COMPLETE to restore, is %s", info.State), false, nil)
	}
	info.State = StateRestoring
	restoring := *info
	tabletIDs := append([]string(nil), info.TabletIDs...)
	c.mu.Unlock()
	if err := c.store.SaveSnapshot(ctx, restoring); err != nil {
		return err
	}

	if err := c.fanOut(ctx, tabletIDs, func(ctx context.Context, tabletID string) error {
		return c.rpc.RestoreTabletSnapshot(ctx, snapshotID, tabletID)
	}); err != nil {
		c.mu.Lock()
		info.State = StateFailed
		info.Error = err.Error()
		failed := *info
		c.mu.Unlock()
		_ = c.store.SaveSnapshot(ctx, failed)
		return err
	}

	c.mu.Lock()
	info.State = StateComplete
	completed := *info
	c.mu.Unlock()
	return c.store.SaveSnapshot(ctx, completed)
}

// GetSnapshot returns a copy of the SnapshotInfo for the given ID.
func (c *Coordinator) GetSnapshot(snapshotID string) (SnapshotInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info, ok := c.snapshots[snapshotID]
	if !ok {
		return SnapshotInfo{}, dberrors.New(dberrors.ErrInvalidArgument, "snapshot not found", false, nil)
	}
	return *info, nil
}

// ListSnapshots returns a copy of all snapshot descriptors.
func (c *Coordinator) ListSnapshots() []SnapshotInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]SnapshotInfo, 0, len(c.snapshots))
	for _, s := range c.snapshots {
		out = append(out, *s)
	}
	return out
}

// Recover reloads snapshots from persistent store and re-fan-outs any
// in-progress snapshots left in CREATING/RESTORING/DELETING states.
func (c *Coordinator) Recover(ctx context.Context) error {
	items, err := c.store.LoadSnapshots(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.snapshots = make(map[string]*SnapshotInfo, len(items))
	for i := range items {
		cp := items[i]
		c.snapshots[cp.SnapshotID] = &cp
	}
	c.mu.Unlock()

	for _, s := range items {
		snap := s
		switch snap.State {
		case StateCreating:
			if err := c.resumeCreate(ctx, &snap); err != nil {
				continue
			}
		case StateRestoring:
			if err := c.resumeRestore(ctx, &snap); err != nil {
				continue
			}
		case StateDeleting:
			if err := c.resumeDelete(ctx, &snap); err != nil {
				continue
			}
		}
	}
	return nil
}

func (c *Coordinator) resumeCreate(ctx context.Context, snap *SnapshotInfo) error {
	err := c.fanOut(ctx, snap.TabletIDs, func(ctx context.Context, tabletID string) error {
		return c.rpc.CreateTabletSnapshot(ctx, snap.SnapshotID, tabletID)
	})
	c.mu.Lock()
	info, ok := c.snapshots[snap.SnapshotID]
	if ok {
		if err != nil {
			info.State = StateFailed
			info.Error = err.Error()
		} else {
			info.State = StateComplete
			info.Error = ""
		}
	}
	updated := SnapshotInfo{}
	if ok {
		updated = *info
	}
	c.mu.Unlock()
	if ok {
		_ = c.store.SaveSnapshot(ctx, updated)
	}
	return err
}

func (c *Coordinator) resumeRestore(ctx context.Context, snap *SnapshotInfo) error {
	err := c.fanOut(ctx, snap.TabletIDs, func(ctx context.Context, tabletID string) error {
		return c.rpc.RestoreTabletSnapshot(ctx, snap.SnapshotID, tabletID)
	})
	c.mu.Lock()
	info, ok := c.snapshots[snap.SnapshotID]
	if ok {
		if err != nil {
			info.State = StateFailed
			info.Error = err.Error()
		} else {
			info.State = StateComplete
			info.Error = ""
		}
	}
	updated := SnapshotInfo{}
	if ok {
		updated = *info
	}
	c.mu.Unlock()
	if ok {
		_ = c.store.SaveSnapshot(ctx, updated)
	}
	return err
}

func (c *Coordinator) resumeDelete(ctx context.Context, snap *SnapshotInfo) error {
	err := c.fanOut(ctx, snap.TabletIDs, func(ctx context.Context, tabletID string) error {
		return c.rpc.DeleteTabletSnapshot(ctx, snap.SnapshotID, tabletID)
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.snapshots, snap.SnapshotID)
	c.mu.Unlock()
	_ = c.store.DeleteSnapshot(ctx, snap.SnapshotID)
	return nil
}

// fanOut issues f(ctx, tabletID) for each tabletID, using up to
// cfg.MaxConcurrentSnapshots goroutines.  Returns the first error encountered.
func (c *Coordinator) fanOut(ctx context.Context, tabletIDs []string, f func(context.Context, string) error) error {
	sem := make(chan struct{}, c.cfg.MaxConcurrentSnapshots)
	errs := make(chan error, len(tabletIDs))
	var wg sync.WaitGroup

	for _, tid := range tabletIDs {
		wg.Add(1)
		sem <- struct{}{}
		go func(tabletID string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := f(ctx, tabletID); err != nil {
				errs <- err
			}
		}(tid)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// noopTabletSnapshotRPC is a no-op implementation of TabletSnapshotRPC for testing.
type noopTabletSnapshotRPC struct{}

func (noopTabletSnapshotRPC) CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return nil
}

func (noopTabletSnapshotRPC) DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return nil
}

func (noopTabletSnapshotRPC) RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return nil
}
