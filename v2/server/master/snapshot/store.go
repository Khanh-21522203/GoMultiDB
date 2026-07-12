package snapshot

import (
	"context"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/infra/storage/rocks"
)

// RocksSnapshotStore is a durable SnapshotStore backed by an
// infra/storage/rocks.Store.
//
// Key schema (RegularDB):
//
//	snapshot/<snapshot_id> -> JSON-encoded SnapshotInfo
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type RocksSnapshotStore struct {
	db rocks.Store
}

var _ SnapshotStore = (*RocksSnapshotStore)(nil)

// NewRocksSnapshotStore creates a RocksSnapshotStore using the provided
// rocks.Store.
func NewRocksSnapshotStore(db rocks.Store) *RocksSnapshotStore {
	return &RocksSnapshotStore{}
}

// SaveSnapshot persists a snapshot descriptor to the store.
//
// Not yet implemented in this scaffold.
func (s *RocksSnapshotStore) SaveSnapshot(ctx context.Context, info SnapshotInfo) error {
	return errors.ErrNotImplemented
}

// DeleteSnapshot removes a snapshot descriptor from the store.
//
// Not yet implemented in this scaffold.
func (s *RocksSnapshotStore) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	return errors.ErrNotImplemented
}

// LoadSnapshots scans all snapshot/* keys and reconstructs every
// SnapshotInfo entry.
//
// Not yet implemented in this scaffold.
func (s *RocksSnapshotStore) LoadSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	return nil, errors.ErrNotImplemented
}
