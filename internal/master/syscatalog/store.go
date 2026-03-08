// Package syscatalog provides a durable CatalogStore implementation backed by
// the rocks.Store KV interface.
//
// Key schema (RegularDB):
//
//	entity/table/<table_id>   → JSON-encoded TableInfo
//	entity/tablet/<tablet_id> → JSON-encoded TabletInfo
//	reqlog/<request_id>       → result hash (for idempotent dedupe)
//
// Mutations are applied atomically via WriteBatch.  LoadSnapshot scans all
// "entity/" keys and reconstructs the full CatalogSnapshot.
package syscatalog

import (
	"context"
	"encoding/json"
	"fmt"

	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/master/catalog"
	"GoMultiDB/internal/storage/rocks"
)

const (
	prefixEntityTable  = "entity/table/"
	prefixEntityTablet = "entity/tablet/"
	prefixReqLog       = "reqlog/"
)

// SysCatalogStore is a durable CatalogStore backed by rocks.Store.
type SysCatalogStore struct {
	db rocks.Store
}

// NewSysCatalogStore creates a SysCatalogStore using the provided rocks.Store.
func NewSysCatalogStore(db rocks.Store) *SysCatalogStore {
	return &SysCatalogStore{db: db}
}

// Apply writes all EntityOps in the mutation atomically.
// It also records the request ID in the reqlog for idempotent replay detection.
func (s *SysCatalogStore) Apply(ctx context.Context, m catalog.CatalogMutation) error {
	wb := rocks.WriteBatch{}

	// Idempotency marker: write the request ID to reqlog.
	if m.RequestID != "" {
		reqKey := []byte(prefixReqLog + string(m.RequestID))
		reqVal := []byte("1")
		if m.RequestKind != "" || m.RequestFingerprint != "" || m.RequestValue != "" {
			reqVal = []byte(m.RequestKind + "|" + m.RequestFingerprint + "|" + m.RequestValue)
		}
		wb.Ops = append(wb.Ops, rocks.KV{Key: reqKey, Value: reqVal})
	}

	for _, t := range m.UpsertTable {
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("marshal table %s: %w", t.TableID, err)
		}
		wb.Ops = append(wb.Ops, rocks.KV{
			Key:   []byte(prefixEntityTable + string(t.TableID)),
			Value: b,
		})
	}

	for _, ti := range m.UpsertTablet {
		b, err := json.Marshal(ti)
		if err != nil {
			return fmt.Errorf("marshal tablet %s: %w", ti.TabletID, err)
		}
		wb.Ops = append(wb.Ops, rocks.KV{
			Key:   []byte(prefixEntityTablet + string(ti.TabletID)),
			Value: b,
		})
	}

	return s.db.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

// LoadSnapshot scans all entity/* keys and reconstructs a CatalogSnapshot.
func (s *SysCatalogStore) LoadSnapshot(ctx context.Context) (*catalog.CatalogSnapshot, error) {
	tables := make(map[ids.TableID]catalog.TableInfo)
	nameToID := make(map[string]ids.TableID)
	tablets := make(map[ids.TabletID]catalog.TabletInfo)

	iter, err := s.db.NewIterator(ctx, rocks.RegularDB, []byte("entity/"))
	if err != nil {
		return nil, fmt.Errorf("new iterator: %w", err)
	}
	for iter.Next() {
		item := iter.Item()
		key := string(item.Key)
		switch {
		case len(key) > len(prefixEntityTable) && key[:len(prefixEntityTable)] == prefixEntityTable:
			var t catalog.TableInfo
			if err := json.Unmarshal(item.Value, &t); err != nil {
				return nil, fmt.Errorf("unmarshal table %s: %w", key, err)
			}
			tables[t.TableID] = t
			nameToID[tableNameKey(t.NamespaceID, t.Name)] = t.TableID
		case len(key) > len(prefixEntityTablet) && key[:len(prefixEntityTablet)] == prefixEntityTablet:
			var ti catalog.TabletInfo
			if err := json.Unmarshal(item.Value, &ti); err != nil {
				return nil, fmt.Errorf("unmarshal tablet %s: %w", key, err)
			}
			tablets[ti.TabletID] = ti
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return catalog.NewSnapshot(tables, nameToID, tablets), nil
}

// SeenRequest returns true if the given request ID was previously applied.
func (s *SysCatalogStore) SeenRequest(ctx context.Context, reqID ids.RequestID) (bool, error) {
	_, ok, err := s.db.Get(ctx, rocks.RegularDB, []byte(prefixReqLog+string(reqID)))
	return ok, err
}

// RequestValue returns the persisted reqlog payload for a request ID.
func (s *SysCatalogStore) RequestValue(ctx context.Context, reqID ids.RequestID) ([]byte, bool, error) {
	v, ok, err := s.db.Get(ctx, rocks.RegularDB, []byte(prefixReqLog+string(reqID)))
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}

func tableNameKey(namespaceID, name string) string {
	return namespaceID + "\x00" + name
}
