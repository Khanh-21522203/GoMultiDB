package catalog

import (
	"context"
	"sync"
)

type TabletReplicaStatus struct {
	TSUUID    string
	LastSeqNo uint64
}

type TabletPlacementView struct {
	TabletID      string
	Replicas      map[string]TabletReplicaStatus
	PrimaryTSUUID string
	Tombstoned    bool
	LastUpdated   uint64
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
			view = TabletPlacementView{TabletID: tabletID, Replicas: make(map[string]TabletReplicaStatus)}
		}
		if view.Replicas == nil {
			view.Replicas = make(map[string]TabletReplicaStatus)
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
			view = TabletPlacementView{TabletID: tabletID, Replicas: make(map[string]TabletReplicaStatus)}
		}
		if view.Replicas == nil {
			view.Replicas = make(map[string]TabletReplicaStatus)
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
	out := TabletPlacementView{
		TabletID:      v.TabletID,
		Replicas:      make(map[string]TabletReplicaStatus, len(v.Replicas)),
		PrimaryTSUUID: v.PrimaryTSUUID,
		Tombstoned:    v.Tombstoned,
		LastUpdated:   v.LastUpdated,
	}
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
		cp := TabletPlacementView{
			TabletID:      v.TabletID,
			Replicas:      make(map[string]TabletReplicaStatus, len(v.Replicas)),
			PrimaryTSUUID: v.PrimaryTSUUID,
			Tombstoned:    v.Tombstoned,
			LastUpdated:   v.LastUpdated,
		}
		for k, r := range v.Replicas {
			cp.Replicas[k] = r
		}
		out = append(out, cp)
	}
	return out
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
