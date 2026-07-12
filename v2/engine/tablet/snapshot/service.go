package snapshot

import (
	rpcpkg "GoMultiDB/v2/infra/rpc"
)

// CreateTabletSnapshotRequest is the request payload for creating a tablet
// snapshot.
type CreateTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
	CreateHT   uint64 `json:"create_ht"`
}

// CreateTabletSnapshotResponse is the response payload for creating a
// tablet snapshot.
type CreateTabletSnapshotResponse struct {
	SnapshotID string `json:"snapshot_id"`
}

// DeleteTabletSnapshotRequest is the request payload for deleting a tablet
// snapshot.
type DeleteTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
}

// DeleteTabletSnapshotResponse is the response payload for deleting a
// tablet snapshot.
type DeleteTabletSnapshotResponse struct{}

// RestoreTabletSnapshotRequest is the request payload for restoring a
// tablet snapshot.
type RestoreTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
}

// RestoreTabletSnapshotResponse is the response payload for restoring a
// tablet snapshot.
type RestoreTabletSnapshotResponse struct{}

// Service exposes tablet snapshot operations via JSON-RPC.
//
// Scaffold stub: Name and Methods panic.
type Service struct {
	store *Store
}

// NewService creates a new tablet snapshot RPC service. Panics if store is
// nil.
func NewService(store *Store) *Service {
	return &Service{}
}

// Name returns the service name for RPC registration.
//
// Not yet implemented in this scaffold.
func (s *Service) Name() string {
	panic("not implemented")
}

// Methods returns the RPC method handlers.
//
// Not yet implemented in this scaffold.
func (s *Service) Methods() map[string]rpcpkg.HandlerFunc {
	panic("not implemented")
}
