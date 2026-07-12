package snapshot

import (
	"context"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/types"

	rpcpkg "GoMultiDB/v2/infra/rpc"
)

// CreateSnapshotRequest is the request payload for creating a distributed
// snapshot.
type CreateSnapshotRequest struct {
	SnapshotID string   `json:"snapshot_id"`
	TabletIDs  []string `json:"tablet_ids"`
	CreateHT   uint64   `json:"create_ht"`
}

// CreateSnapshotResponse is the response payload for creating a
// distributed snapshot.
type CreateSnapshotResponse struct {
	SnapshotInfo *SnapshotInfo `json:"snapshot_info,omitempty"`
}

// DeleteSnapshotRequest is the request payload for deleting a distributed
// snapshot.
type DeleteSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

// DeleteSnapshotResponse is the response payload for deleting a
// distributed snapshot.
type DeleteSnapshotResponse struct{}

// RestoreSnapshotRequest is the request payload for restoring a
// distributed snapshot.
type RestoreSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

// RestoreSnapshotResponse is the response payload for restoring a
// distributed snapshot.
type RestoreSnapshotResponse struct{}

// GetSnapshotRequest is the request payload for fetching a snapshot's
// descriptor.
type GetSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

// GetSnapshotResponse is the response payload for fetching a snapshot's
// descriptor.
type GetSnapshotResponse struct {
	SnapshotInfo *SnapshotInfo `json:"snapshot_info,omitempty"`
}

// ListSnapshotsRequest is the (empty) request payload for listing all
// snapshots.
type ListSnapshotsRequest struct{}

// ListSnapshotsResponse is the response payload for listing all snapshots.
type ListSnapshotsResponse struct {
	Snapshots []SnapshotInfo `json:"snapshots,omitempty"`
}

// Service exposes Coordinator operations via JSON-RPC.
//
// Scaffold stub: Name and Methods panic.
type Service struct {
	coordinator *Coordinator
}

// NewService creates a new snapshot RPC service. Panics if coord is nil.
func NewService(coord *Coordinator) *Service {
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

// createSnapshot is the RPC handler for CreateSnapshotRequest.
func (s *Service) createSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}

// deleteSnapshot is the RPC handler for DeleteSnapshotRequest.
func (s *Service) deleteSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}

// restoreSnapshot is the RPC handler for RestoreSnapshotRequest.
func (s *Service) restoreSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}

// getSnapshot is the RPC handler for GetSnapshotRequest.
func (s *Service) getSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}

// listSnapshots is the RPC handler for ListSnapshotsRequest.
func (s *Service) listSnapshots(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}
