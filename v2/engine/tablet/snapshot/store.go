package snapshot

import (
	"context"
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/infra/storage/rocks"
)

// snapshotKeyPrefix namespaces snapshot metadata and data keys within the
// regular database so they are excluded from ordinary scans.
const snapshotKeyPrefix = "snapshot/"

// SnapshotData represents a snapshot of tablet data at a point in time.
type SnapshotData struct {
	SnapshotID  string
	TabletID    string
	CreatedAt   time.Time
	CreateHT    uint64
	RegularData []KVPair
	IntentsData []KVPair
}

// KVPair represents a key-value pair from the store.
type KVPair struct {
	Key   []byte
	Value []byte
}

// Store manages tablet snapshots, persisting them within an
// infra/storage/rocks.Store alongside an in-memory index for fast listing.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Store struct {
	mu         sync.RWMutex
	rocksStore rocks.Store
	snapshots  map[string]*SnapshotData
}

// NewStore creates a new snapshot store backed by a rocks.Store.
func NewStore(rs rocks.Store) *Store {
	return &Store{}
}

// CreateSnapshot creates a snapshot of the tablet data, copying all data
// from both RegularDB and IntentsDB. Returns ErrConflict if snapshotID
// already exists.
//
// Not yet implemented in this scaffold.
func (s *Store) CreateSnapshot(ctx context.Context, snapshotID, tabletID string, createHT uint64) (*SnapshotData, error) {
	return nil, errors.ErrNotImplemented
}

// DeleteSnapshot removes a snapshot's persisted data and metadata.
//
// Not yet implemented in this scaffold.
func (s *Store) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return errors.ErrNotImplemented
}

// RestoreSnapshot restores data from a snapshot into the live store. This
// is intended to be atomic: either all data is restored or none.
//
// Not yet implemented in this scaffold.
func (s *Store) RestoreSnapshot(ctx context.Context, snapshotID string) error {
	return errors.ErrNotImplemented
}

// GetSnapshot retrieves snapshot metadata.
//
// Not yet implemented in this scaffold.
func (s *Store) GetSnapshot(ctx context.Context, snapshotID string) (*SnapshotData, error) {
	return nil, errors.ErrNotImplemented
}

// ListSnapshots returns all known snapshots.
//
// Not yet implemented in this scaffold.
func (s *Store) ListSnapshots(ctx context.Context) []*SnapshotData {
	panic("not implemented")
}

// Recover reloads snapshot metadata from persistent storage, replacing the
// in-memory index.
//
// Not yet implemented in this scaffold.
func (s *Store) Recover(ctx context.Context) error {
	return errors.ErrNotImplemented
}
