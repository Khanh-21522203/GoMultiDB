// Package snapshot provides the RPC service for distributed snapshot operations.
package snapshot

import (
	"context"
	"encoding/json"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/types"
	rpcpkg "GoMultiDB/internal/rpc"
)

// Request types for snapshot RPC operations.

type CreateSnapshotRequest struct {
	SnapshotID string   `json:"snapshot_id"`
	TabletIDs  []string `json:"tablet_ids"`
	CreateHT   uint64   `json:"create_ht"`
}

type CreateSnapshotResponse struct {
	SnapshotInfo *SnapshotInfo `json:"snapshot_info,omitempty"`
}

type DeleteSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

type DeleteSnapshotResponse struct{}

type RestoreSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

type RestoreSnapshotResponse struct{}

type GetSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
}

type GetSnapshotResponse struct {
	SnapshotInfo *SnapshotInfo `json:"snapshot_info,omitempty"`
}

type ListSnapshotsRequest struct{}

type ListSnapshotsResponse struct {
	Snapshots []SnapshotInfo `json:"snapshots,omitempty"`
}

// Service exposes snapshot coordinator operations via JSON-RPC.
type Service struct {
	coordinator *Coordinator
}

// NewService creates a new snapshot RPC service.
func NewService(coord *Coordinator) *Service {
	if coord == nil {
		panic("coordinator is required")
	}
	return &Service{coordinator: coord}
}

// Name returns the service name for RPC registration.
func (s *Service) Name() string {
	return "snapshot"
}

// Methods returns the RPC method handlers.
func (s *Service) Methods() map[string]rpcpkg.HandlerFunc {
	return map[string]rpcpkg.HandlerFunc{
		"create_snapshot": s.createSnapshot,
		"delete_snapshot": s.deleteSnapshot,
		"restore_snapshot": s.restoreSnapshot,
		"get_snapshot":     s.getSnapshot,
		"list_snapshots":   s.listSnapshots,
	}
}

func (s *Service) createSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req CreateSnapshotRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid create snapshot request", false, err)
	}

	info, err := s.coordinator.CreateSnapshot(ctx, req.SnapshotID, req.TabletIDs, req.CreateHT)
	if err != nil {
		return nil, err
	}

	resp := CreateSnapshotResponse{SnapshotInfo: info}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal create snapshot response", false, err)
	}
	return b, nil
}

func (s *Service) deleteSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req DeleteSnapshotRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid delete snapshot request", false, err)
	}

	if err := s.coordinator.DeleteSnapshot(ctx, req.SnapshotID); err != nil {
		return nil, err
	}

	resp := DeleteSnapshotResponse{}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal delete snapshot response", false, err)
	}
	return b, nil
}

func (s *Service) restoreSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req RestoreSnapshotRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid restore snapshot request", false, err)
	}

	if err := s.coordinator.RestoreSnapshot(ctx, req.SnapshotID); err != nil {
		return nil, err
	}

	resp := RestoreSnapshotResponse{}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal restore snapshot response", false, err)
	}
	return b, nil
}

func (s *Service) getSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req GetSnapshotRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid get snapshot request", false, err)
	}

	info, err := s.coordinator.GetSnapshot(req.SnapshotID)
	if err != nil {
		return nil, err
	}

	resp := GetSnapshotResponse{SnapshotInfo: &info}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal get snapshot response", false, err)
	}
	return b, nil
}

func (s *Service) listSnapshots(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req ListSnapshotsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid list snapshots request", false, err)
	}

	snapshots := s.coordinator.ListSnapshots()
	resp := ListSnapshotsResponse{Snapshots: snapshots}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal list snapshots response", false, err)
	}
	return b, nil
}
