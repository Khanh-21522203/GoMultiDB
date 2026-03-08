// Package snapshot provides the RPC service for tablet snapshot operations.
package snapshot

import (
	"context"
	"encoding/json"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/types"
	rpcpkg "GoMultiDB/internal/rpc"
)

// Request and response types for tablet snapshot RPC operations.

type CreateTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
	CreateHT   uint64 `json:"create_ht"`
}

type CreateTabletSnapshotResponse struct {
	SnapshotID string `json:"snapshot_id"`
}

type DeleteTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
}

type DeleteTabletSnapshotResponse struct{}

type RestoreTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
}

type RestoreTabletSnapshotResponse struct{}

// Service exposes tablet snapshot operations via JSON-RPC.
type Service struct {
	store *Store
}

// NewService creates a new tablet snapshot RPC service.
func NewService(store *Store) *Service {
	if store == nil {
		panic("store is required")
	}
	return &Service{store: store}
}

// Name returns the service name for RPC registration.
func (s *Service) Name() string {
	return "tablet_snapshot"
}

// Methods returns the RPC method handlers.
func (s *Service) Methods() map[string]rpcpkg.HandlerFunc {
	return map[string]rpcpkg.HandlerFunc{
		"create_tablet_snapshot": s.createTabletSnapshot,
		"delete_tablet_snapshot": s.deleteTabletSnapshot,
		"restore_tablet_snapshot": s.restoreTabletSnapshot,
	}
}

func (s *Service) createTabletSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req CreateTabletSnapshotRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid create tablet snapshot request", false, err)
	}

	data, err := s.store.CreateSnapshot(ctx, req.SnapshotID, req.TabletID, req.CreateHT)
	if err != nil {
		return nil, err
	}

	resp := CreateTabletSnapshotResponse{SnapshotID: data.SnapshotID}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal create tablet snapshot response", false, err)
	}
	return b, nil
}

func (s *Service) deleteTabletSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req DeleteTabletSnapshotRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid delete tablet snapshot request", false, err)
	}

	if err := s.store.DeleteSnapshot(ctx, req.SnapshotID); err != nil {
		return nil, err
	}

	resp := DeleteTabletSnapshotResponse{}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal delete tablet snapshot response", false, err)
	}
	return b, nil
}

func (s *Service) restoreTabletSnapshot(ctx context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	var req RestoreTabletSnapshotRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid restore tablet snapshot request", false, err)
	}

	if err := s.store.RestoreSnapshot(ctx, req.SnapshotID); err != nil {
		return nil, err
	}

	resp := RestoreTabletSnapshotResponse{}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrInternal, "marshal restore tablet snapshot response", false, err)
	}
	return b, nil
}
