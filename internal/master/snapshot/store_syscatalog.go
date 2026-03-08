package snapshot

import (
	"context"
	"encoding/json"
	"fmt"

	"GoMultiDB/internal/storage/rocks"
)

const (
	snapshotEntityPrefix = "entity/snapshot/"
)

// SysCatalogStore persists snapshot descriptors in sys.catalog (rocks RegularDB).
type SysCatalogStore struct {
	db rocks.Store
}

func NewSysCatalogStore(db rocks.Store) *SysCatalogStore {
	return &SysCatalogStore{db: db}
}

func (s *SysCatalogStore) SaveSnapshot(ctx context.Context, info SnapshotInfo) error {
	b, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal snapshot %s: %w", info.SnapshotID, err)
	}
	wb := rocks.WriteBatch{Ops: []rocks.KV{{
		Key:   []byte(snapshotEntityPrefix + info.SnapshotID),
		Value: b,
	}}}
	return s.db.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

func (s *SysCatalogStore) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	wb := rocks.WriteBatch{Ops: []rocks.KV{{
		Key:   []byte(snapshotEntityPrefix + snapshotID),
		Value: nil,
	}}}
	return s.db.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

func (s *SysCatalogStore) LoadSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	it, err := s.db.NewIterator(ctx, rocks.RegularDB, []byte(snapshotEntityPrefix))
	if err != nil {
		return nil, fmt.Errorf("new iterator: %w", err)
	}
	out := make([]SnapshotInfo, 0)
	for it.Next() {
		item := it.Item()
		var info SnapshotInfo
		if err := json.Unmarshal(item.Value, &info); err != nil {
			return nil, fmt.Errorf("unmarshal snapshot key %s: %w", string(item.Key), err)
		}
		out = append(out, info)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}
	return out, nil
}
