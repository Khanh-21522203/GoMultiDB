// Package snapshot provides a durable SnapshotStore implementation backed by
// the rocks.Store KV interface.
//
// Key schema (RegularDB):
//
//	snapshot/<snapshot_id> → JSON-encoded SnapshotInfo
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"

	"GoMultiDB/internal/storage/rocks"
)

const (
	prefixSnapshot = "snapshot/"
)

// RocksSnapshotStore is a durable SnapshotStore backed by rocks.Store.
type RocksSnapshotStore struct {
	db rocks.Store
}

// NewRocksSnapshotStore creates a RocksSnapshotStore using the provided rocks.Store.
func NewRocksSnapshotStore(db rocks.Store) *RocksSnapshotStore {
	return &RocksSnapshotStore{db: db}
}

// SaveSnapshot persists a snapshot descriptor to the store.
func (s *RocksSnapshotStore) SaveSnapshot(ctx context.Context, info SnapshotInfo) error {
	b, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal snapshot %s: %w", info.SnapshotID, err)
	}
	wb := rocks.WriteBatch{
		Ops: []rocks.KV{{
			Key:   []byte(prefixSnapshot + info.SnapshotID),
			Value: b,
		}},
	}
	return s.db.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

// DeleteSnapshot removes a snapshot descriptor from the store.
func (s *RocksSnapshotStore) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	wb := rocks.WriteBatch{
		Ops: []rocks.KV{{
			Key:   []byte(prefixSnapshot + snapshotID),
			Value: nil, // nil value indicates deletion
		}},
	}
	return s.db.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

// LoadSnapshots scans all snapshot/* keys and reconstructs all SnapshotInfo entries.
func (s *RocksSnapshotStore) LoadSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	iter, err := s.db.NewIterator(ctx, rocks.RegularDB, []byte(prefixSnapshot))
	if err != nil {
		return nil, fmt.Errorf("new iterator: %w", err)
	}
	defer func() {
		// Note: MemoryStore iterator doesn't need explicit close, but real rocksdb would.
		_ = iter
	}()

	var infos []SnapshotInfo
	for iter.Next() {
		item := iter.Item()
		var info SnapshotInfo
		if err := json.Unmarshal(item.Value, &info); err != nil {
			return nil, fmt.Errorf("unmarshal snapshot %s: %w", string(item.Key), err)
		}
		infos = append(infos, info)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}
	return infos, nil
}
