package syscatalog

import (
	"context"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/ids"
	"GoMultiDB/v2/infra/storage/rocks"
	"GoMultiDB/v2/server/master/catalog"
)

// SysCatalogStore is a durable master/catalog.CatalogStore backed by an
// infra/storage/rocks.Store.
//
// Key schema (RegularDB):
//
//	entity/table/<table_id>   -> JSON-encoded catalog.TableInfo
//	entity/tablet/<tablet_id> -> JSON-encoded catalog.TabletInfo
//	reqlog/<request_id>       -> result value (for idempotent dedupe)
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type SysCatalogStore struct {
	db rocks.Store
}

// NewSysCatalogStore creates a SysCatalogStore using the provided
// rocks.Store.
func NewSysCatalogStore(db rocks.Store) *SysCatalogStore {
	return &SysCatalogStore{}
}

// Apply writes all table and tablet upserts in m atomically, and records
// m.RequestID in the reqlog for idempotent replay detection.
//
// Not yet implemented in this scaffold.
func (s *SysCatalogStore) Apply(ctx context.Context, m catalog.CatalogMutation) error {
	return errors.ErrNotImplemented
}

// LoadSnapshot scans all entity/* keys and reconstructs a
// catalog.CatalogSnapshot.
//
// Not yet implemented in this scaffold.
func (s *SysCatalogStore) LoadSnapshot(ctx context.Context) (*catalog.CatalogSnapshot, error) {
	return nil, errors.ErrNotImplemented
}

// SeenRequest returns true if the given request ID was previously applied.
//
// Not yet implemented in this scaffold.
func (s *SysCatalogStore) SeenRequest(ctx context.Context, reqID ids.RequestID) (bool, error) {
	return false, errors.ErrNotImplemented
}

// RequestValue returns the persisted reqlog payload for a request ID.
//
// Not yet implemented in this scaffold.
func (s *SysCatalogStore) RequestValue(ctx context.Context, reqID ids.RequestID) ([]byte, bool, error) {
	return nil, false, errors.ErrNotImplemented
}
