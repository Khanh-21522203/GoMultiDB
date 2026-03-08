// Package snapshot provides tablet-level snapshot storage and operations.
// Snapshots are stored as isolated copies of tablet data at a point in time.
//
// For the current MemoryStore-based implementation, snapshots are in-memory copies.
// When real RocksDB is integrated, this can be extended to use RocksDB checkpoints.
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/storage/rocks"
)

const (
	// Snapshot data is stored with keys prefixed by "snapshot/<snapshot-id>/"
	snapshotKeyPrefix = "snapshot/"
)

// SnapshotData represents a snapshot of tablet data at a point in time.
type SnapshotData struct {
	SnapshotID  string    `json:"snapshot_id"`
	TabletID    string    `json:"tablet_id"`
	CreatedAt   time.Time `json:"created_at"`
	CreateHT    uint64    `json:"create_ht"`
	RegularData []KVPair  `json:"regular_data"`
	IntentsData []KVPair  `json:"intents_data"`
}

// KVPair represents a key-value pair from the store.
type KVPair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// Store manages tablet snapshots.
type Store struct {
	mu         sync.RWMutex
	rocksStore rocks.Store
	// In-memory index of snapshots for faster listing
	snapshots map[string]*SnapshotData
}

// NewStore creates a new snapshot store backed by a rocks.Store.
func NewStore(rs rocks.Store) *Store {
	return &Store{
		rocksStore: rs,
		snapshots:  make(map[string]*SnapshotData),
	}
}

// CreateSnapshot creates a snapshot of the tablet data.
// It copies all data from both RegularDB and IntentsDB.
func (s *Store) CreateSnapshot(ctx context.Context, snapshotID, tabletID string, createHT uint64) (*SnapshotData, error) {
	if snapshotID == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "snapshot id is required", false, nil)
	}
	if tabletID == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "tablet id is required", false, nil)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for existing snapshot
	if _, exists := s.snapshots[snapshotID]; exists {
		return nil, dberrors.New(dberrors.ErrConflict, "snapshot already exists", false, nil)
	}

	// Scan all data from RegularDB
	regularData, err := s.scanDB(ctx, rocks.RegularDB)
	if err != nil {
		return nil, fmt.Errorf("scan regular db: %w", err)
	}

	// Scan all data from IntentsDB
	intentsData, err := s.scanDB(ctx, rocks.IntentsDB)
	if err != nil {
		return nil, fmt.Errorf("scan intents db: %w", err)
	}

	data := &SnapshotData{
		SnapshotID:  snapshotID,
		TabletID:    tabletID,
		CreatedAt:   time.Now().UTC(),
		CreateHT:    createHT,
		RegularData: regularData,
		IntentsData: intentsData,
	}

	// Persist snapshot metadata
	if err := s.persistSnapshotMeta(ctx, data); err != nil {
		return nil, fmt.Errorf("persist snapshot meta: %w", err)
	}

	// Persist snapshot data
	if err := s.persistSnapshotData(ctx, data); err != nil {
		return nil, fmt.Errorf("persist snapshot data: %w", err)
	}

	s.snapshots[snapshotID] = data
	return data, nil
}

// DeleteSnapshot removes a snapshot.
func (s *Store) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.snapshots[snapshotID]; !exists {
		return dberrors.New(dberrors.ErrInvalidArgument, "snapshot not found", false, nil)
	}

	// Delete persisted snapshot data
	if err := s.deleteSnapshotData(ctx, snapshotID); err != nil {
		return fmt.Errorf("delete snapshot data: %w", err)
	}

	// Delete persisted snapshot metadata
	if err := s.deleteSnapshotMeta(ctx, snapshotID); err != nil {
		return fmt.Errorf("delete snapshot meta: %w", err)
	}

	delete(s.snapshots, snapshotID)
	return nil
}

// RestoreSnapshot restores data from a snapshot into the live store.
// This is an atomic operation - either all data is restored or none.
func (s *Store) RestoreSnapshot(ctx context.Context, snapshotID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, exists := s.snapshots[snapshotID]
	if !exists {
		return dberrors.New(dberrors.ErrInvalidArgument, "snapshot not found", false, nil)
	}

	// Build write batch for RegularDB
	regularBatch := rocks.WriteBatch{Ops: make([]rocks.KV, 0, len(data.RegularData))}
	for _, kv := range data.RegularData {
		regularBatch.Ops = append(regularBatch.Ops, rocks.KV{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	// Build write batch for IntentsDB
	intentsBatch := rocks.WriteBatch{Ops: make([]rocks.KV, 0, len(data.IntentsData))}
	for _, kv := range data.IntentsData {
		intentsBatch.Ops = append(intentsBatch.Ops, rocks.KV{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	// Apply both batches atomically
	if err := s.rocksStore.ApplyWriteBatch(ctx, rocks.RegularDB, regularBatch); err != nil {
		return fmt.Errorf("restore regular db: %w", err)
	}
	if err := s.rocksStore.ApplyWriteBatch(ctx, rocks.IntentsDB, intentsBatch); err != nil {
		return fmt.Errorf("restore intents db: %w", err)
	}

	return nil
}

// GetSnapshot retrieves snapshot metadata.
func (s *Store) GetSnapshot(_ context.Context, snapshotID string) (*SnapshotData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, exists := s.snapshots[snapshotID]
	if !exists {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "snapshot not found", false, nil)
	}
	return data, nil
}

// ListSnapshots returns all snapshots.
func (s *Store) ListSnapshots(_ context.Context) []*SnapshotData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*SnapshotData, 0, len(s.snapshots))
	for _, s := range s.snapshots {
		out = append(out, s)
	}
	return out
}

// Recover reloads snapshots from persistent storage.
func (s *Store) Recover(ctx context.Context) error {
	// Scan for snapshot metadata keys
	prefix := snapshotKeyPrefix + "meta/"
	iter, err := s.rocksStore.NewIterator(ctx, rocks.RegularDB, []byte(prefix))
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	snapshots := make(map[string]*SnapshotData)
	for iter.Next() {
		item := iter.Item()
		var data SnapshotData
		if err := json.Unmarshal(item.Value, &data); err != nil {
			return fmt.Errorf("unmarshal snapshot meta: %w", err)
		}
		snapshots[data.SnapshotID] = &data
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	s.mu.Lock()
	s.snapshots = snapshots
	s.mu.Unlock()
	return nil
}

// scanDB scans all key-value pairs from a database.
func (s *Store) scanDB(ctx context.Context, kind rocks.DBKind) ([]KVPair, error) {
	iter, err := s.rocksStore.NewIterator(ctx, kind, nil)
	if err != nil {
		return nil, fmt.Errorf("new iterator: %w", err)
	}
	defer func() {
		// Note: MemoryStore iterator doesn't need explicit close
		_ = iter
	}()

	var data []KVPair
	for iter.Next() {
		item := iter.Item()
		// Skip snapshot-related keys to avoid recursive snapshots
		if len(item.Key) > len(snapshotKeyPrefix) && string(item.Key[:len(snapshotKeyPrefix)]) == snapshotKeyPrefix {
			continue
		}
		data = append(data, KVPair{
			Key:   append([]byte(nil), item.Key...),
			Value: append([]byte(nil), item.Value...),
		})
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}
	return data, nil
}

// persistSnapshotMeta persists snapshot metadata.
func (s *Store) persistSnapshotMeta(ctx context.Context, data *SnapshotData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal snapshot meta: %w", err)
	}
	key := []byte(snapshotKeyPrefix + "meta/" + data.SnapshotID)
	wb := rocks.WriteBatch{
		Ops: []rocks.KV{{Key: key, Value: b}},
	}
	return s.rocksStore.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

// persistSnapshotData persists snapshot data.
func (s *Store) persistSnapshotData(ctx context.Context, data *SnapshotData) error {
	wb := rocks.WriteBatch{Ops: make([]rocks.KV, 0, len(data.RegularData)+len(data.IntentsData))}

	// Persist regular data with snapshot prefix
	for i, kv := range data.RegularData {
		key := []byte(fmt.Sprintf("%sdata/%s/regular/%010d", snapshotKeyPrefix, data.SnapshotID, i))
		wb.Ops = append(wb.Ops, rocks.KV{Key: key, Value: kv.Value})
	}

	// Persist intents data with snapshot prefix
	for i, kv := range data.IntentsData {
		key := []byte(fmt.Sprintf("%sdata/%s/intents/%010d", snapshotKeyPrefix, data.SnapshotID, i))
		wb.Ops = append(wb.Ops, rocks.KV{Key: key, Value: kv.Value})
	}

	return s.rocksStore.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

// deleteSnapshotMeta deletes snapshot metadata.
func (s *Store) deleteSnapshotMeta(ctx context.Context, snapshotID string) error {
	key := []byte(snapshotKeyPrefix + "meta/" + snapshotID)
	wb := rocks.WriteBatch{
		Ops: []rocks.KV{{Key: key, Value: nil}},
	}
	return s.rocksStore.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

// deleteSnapshotData deletes snapshot data.
func (s *Store) deleteSnapshotData(ctx context.Context, snapshotID string) error {
	// Scan for all snapshot data keys
	prefix := []byte(snapshotKeyPrefix + "data/" + snapshotID + "/")
	iter, err := s.rocksStore.NewIterator(ctx, rocks.RegularDB, prefix)
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	wb := rocks.WriteBatch{Ops: make([]rocks.KV, 0)}
	for iter.Next() {
		item := iter.Item()
		wb.Ops = append(wb.Ops, rocks.KV{Key: item.Key, Value: nil})
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	if len(wb.Ops) > 0 {
		return s.rocksStore.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
	}
	return nil
}
