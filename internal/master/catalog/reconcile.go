package catalog

import (
	"context"
	"fmt"
	"sync"
)

type TransferState string

const (
	TransferStateNone     TransferState = "NONE"
	TransferStatePrepared TransferState = "TRANSFER_PREPARED"
)

type TabletReplicaStatus struct {
	TSUUID    string
	LastSeqNo uint64
}

type TabletPlacementView struct {
	TabletID             string
	Replicas             map[string]TabletReplicaStatus
	PrimaryTSUUID        string
	Tombstoned           bool
	LastUpdated          uint64
	OwnerEpoch           uint64
	TransferState        TransferState
	TransferEpoch        uint64
	PendingPrimaryTSUUID string
}

type MemoryReconcileSink struct {
	mu      sync.RWMutex
	tablets map[string]TabletPlacementView
}

func NewMemoryReconcileSink() *MemoryReconcileSink {
	return &MemoryReconcileSink{tablets: make(map[string]TabletPlacementView)}
}

func (s *MemoryReconcileSink) ApplyTabletReport(_ context.Context, delta TabletReportDelta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, tabletID := range delta.Updated {
		view := s.tablets[tabletID]
		if view.TabletID == "" {
			view = TabletPlacementView{
				TabletID:      tabletID,
				Replicas:      make(map[string]TabletReplicaStatus),
				OwnerEpoch:    1,
				TransferState: TransferStateNone,
			}
		}
		if view.Replicas == nil {
			view.Replicas = make(map[string]TabletReplicaStatus)
		}
		if view.OwnerEpoch == 0 {
			view.OwnerEpoch = 1
		}
		if view.TransferState == "" {
			view.TransferState = TransferStateNone
		}
		view.Tombstoned = false
		view.LastUpdated = delta.SequenceNo
		view.Replicas[delta.TSUUID] = TabletReplicaStatus{TSUUID: delta.TSUUID, LastSeqNo: delta.SequenceNo}
		view.PrimaryTSUUID = selectPrimaryReplica(view.Replicas)
		s.tablets[tabletID] = view
	}

	for _, tabletID := range delta.RemovedIDs {
		view := s.tablets[tabletID]
		if view.TabletID == "" {
			view = TabletPlacementView{
				TabletID:      tabletID,
				Replicas:      make(map[string]TabletReplicaStatus),
				OwnerEpoch:    1,
				TransferState: TransferStateNone,
			}
		}
		if view.Replicas == nil {
			view.Replicas = make(map[string]TabletReplicaStatus)
		}
		if view.OwnerEpoch == 0 {
			view.OwnerEpoch = 1
		}
		if view.TransferState == "" {
			view.TransferState = TransferStateNone
		}
		delete(view.Replicas, delta.TSUUID)
		view.LastUpdated = delta.SequenceNo
		if len(view.Replicas) == 0 {
			view.Tombstoned = true
			view.PrimaryTSUUID = ""
		} else {
			view.PrimaryTSUUID = selectPrimaryReplica(view.Replicas)
		}
		s.tablets[tabletID] = view
	}

	return nil
}

func (s *MemoryReconcileSink) GetTablet(tabletID string) (TabletPlacementView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.tablets[tabletID]
	if !ok {
		return TabletPlacementView{}, false
	}
	out := clonePlacementView(v)
	for k, r := range v.Replicas {
		out.Replicas[k] = r
	}
	return out, true
}

func (s *MemoryReconcileSink) ListTablets() []TabletPlacementView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TabletPlacementView, 0, len(s.tablets))
	for _, v := range s.tablets {
		cp := clonePlacementView(v)
		for k, r := range v.Replicas {
			cp.Replicas[k] = r
		}
		out = append(out, cp)
	}
	return out
}

func (s *MemoryReconcileSink) PrepareOwnershipTransfer(_ context.Context, tabletID, targetPrimary string) (TabletPlacementView, error) {
	if targetPrimary == "" {
		return TabletPlacementView{}, fmt.Errorf("target primary is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	view, ok := s.tablets[tabletID]
	if !ok {
		return TabletPlacementView{}, fmt.Errorf("tablet %s not found", tabletID)
	}
	if view.Tombstoned {
		return TabletPlacementView{}, fmt.Errorf("tablet %s is tombstoned", tabletID)
	}
	if view.TransferState == TransferStatePrepared {
		if view.PendingPrimaryTSUUID == targetPrimary {
			return clonePlacementView(view), nil
		}
		return TabletPlacementView{}, fmt.Errorf("tablet %s transfer already prepared for %s", tabletID, view.PendingPrimaryTSUUID)
	}
	if view.PrimaryTSUUID == targetPrimary {
		return TabletPlacementView{}, fmt.Errorf("tablet %s already owned by %s", tabletID, targetPrimary)
	}
	if view.OwnerEpoch == 0 {
		view.OwnerEpoch = 1
	}
	view.TransferState = TransferStatePrepared
	view.PendingPrimaryTSUUID = targetPrimary
	view.TransferEpoch = view.OwnerEpoch + 1
	s.tablets[tabletID] = view
	return clonePlacementView(view), nil
}

func (s *MemoryReconcileSink) CommitOwnershipTransfer(_ context.Context, tabletID string, expectedTransferEpoch uint64) (TabletPlacementView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	view, ok := s.tablets[tabletID]
	if !ok {
		return TabletPlacementView{}, fmt.Errorf("tablet %s not found", tabletID)
	}
	if view.TransferState != TransferStatePrepared || view.PendingPrimaryTSUUID == "" {
		return TabletPlacementView{}, fmt.Errorf("tablet %s transfer is not prepared", tabletID)
	}
	if expectedTransferEpoch != 0 && view.TransferEpoch != expectedTransferEpoch {
		return TabletPlacementView{}, fmt.Errorf("tablet %s transfer epoch mismatch", tabletID)
	}

	view.PrimaryTSUUID = view.PendingPrimaryTSUUID
	if view.TransferEpoch > 0 {
		view.OwnerEpoch = view.TransferEpoch
	} else if view.OwnerEpoch == 0 {
		view.OwnerEpoch = 1
	} else {
		view.OwnerEpoch++
	}
	view.TransferState = TransferStateNone
	view.TransferEpoch = 0
	view.PendingPrimaryTSUUID = ""
	s.tablets[tabletID] = view
	return clonePlacementView(view), nil
}

func clonePlacementView(v TabletPlacementView) TabletPlacementView {
	return TabletPlacementView{
		TabletID:             v.TabletID,
		Replicas:             make(map[string]TabletReplicaStatus, len(v.Replicas)),
		PrimaryTSUUID:        v.PrimaryTSUUID,
		Tombstoned:           v.Tombstoned,
		LastUpdated:          v.LastUpdated,
		OwnerEpoch:           v.OwnerEpoch,
		TransferState:        v.TransferState,
		TransferEpoch:        v.TransferEpoch,
		PendingPrimaryTSUUID: v.PendingPrimaryTSUUID,
	}
}

// selectPrimaryReplica chooses a deterministic primary owner from replicas:
// highest sequence number wins; ties break by lexicographically smallest TSUUID.
func selectPrimaryReplica(replicas map[string]TabletReplicaStatus) string {
	primary := ""
	var maxSeq uint64
	for tsUUID, rep := range replicas {
		if primary == "" || rep.LastSeqNo > maxSeq || (rep.LastSeqNo == maxSeq && tsUUID < primary) {
			primary = tsUUID
			maxSeq = rep.LastSeqNo
		}
	}
	return primary
}
