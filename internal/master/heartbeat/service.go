package heartbeat

import (
	"context"
	"sort"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/master/catalog"
)

type TSInstance struct {
	PermanentUUID string
	InstanceSeqNo uint64
}

type TSRegistration struct {
	RPCAddress  string
	HTTPAddress string
}

type TabletReport struct {
	IsIncremental bool
	SequenceNo    uint64
	Updated       []string
	RemovedIDs    []string
}

type TabletAction struct {
	TabletID string
	Action   string
}

type HeartbeatRequest struct {
	Instance     TSInstance
	Registration *TSRegistration
	TabletReport TabletReport
}

type HeartbeatResponse struct {
	NeedReregister       bool
	NeedFullTabletReport bool
	TabletActions        []TabletAction
}

type TSDescriptor struct {
	Instance          TSInstance
	Registration      TSRegistration
	LastHeartbeatAt   time.Time
	LastReportSeqNo   uint64
	NeedFullTabletRpt bool
}

type TSManager struct {
	mu          sync.RWMutex
	descriptors map[string]TSDescriptor
}

func NewTSManager() *TSManager {
	return &TSManager{descriptors: make(map[string]TSDescriptor)}
}

func (m *TSManager) Get(uuid string) (TSDescriptor, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.descriptors[uuid]
	return d, ok
}

func (m *TSManager) upsert(d TSDescriptor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.descriptors[d.Instance.PermanentUUID] = d
}

func (m *TSManager) ListStale(unresponsiveTimeout time.Duration, now time.Time) []TSDescriptor {
	if unresponsiveTimeout <= 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TSDescriptor, 0)
	for _, d := range m.descriptors {
		if now.Sub(d.LastHeartbeatAt) >= unresponsiveTimeout {
			out = append(out, d)
		}
	}
	return out
}

type Service struct {
	mu                  sync.RWMutex
	isLeader            bool
	ts                  *TSManager
	catalog             *catalog.Manager
	planner             *catalog.DirectivePlanner
	unresponsiveTimeout time.Duration
	nowFn               func() time.Time
}

func NewService(ts *TSManager, cat *catalog.Manager) *Service {
	if ts == nil {
		ts = NewTSManager()
	}
	return &Service{
		ts:                  ts,
		catalog:             cat,
		planner:             catalog.NewDirectivePlanner(3),
		unresponsiveTimeout: 60 * time.Second,
		nowFn:               time.Now,
	}
}

func (s *Service) SetLeader(isLeader bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isLeader = isLeader
	if s.catalog != nil {
		s.catalog.SetLeader(isLeader)
	}
}

func (s *Service) TSHeartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	s.mu.RLock()
	isLeader := s.isLeader
	nowFn := s.nowFn
	cat := s.catalog
	planner := s.planner
	unresponsiveTimeout := s.unresponsiveTimeout
	s.mu.RUnlock()

	if !isLeader {
		return HeartbeatResponse{}, dberrors.New(dberrors.ErrNotLeader, "heartbeat requires master leader", true, nil)
	}
	if req.Instance.PermanentUUID == "" {
		return HeartbeatResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "tserver permanent uuid is required", false, nil)
	}

	now := nowFn().UTC()
	d, exists := s.ts.Get(req.Instance.PermanentUUID)
	reporterWasStale := false
	if exists && unresponsiveTimeout > 0 {
		reporterWasStale = now.Sub(d.LastHeartbeatAt) >= unresponsiveTimeout
	}

	if !exists {
		if req.Registration == nil {
			return HeartbeatResponse{NeedReregister: true}, nil
		}
		d = TSDescriptor{
			Instance:        req.Instance,
			Registration:    *req.Registration,
			LastHeartbeatAt: now,
		}
		if req.TabletReport.IsIncremental {
			d.NeedFullTabletRpt = true
			s.ts.upsert(d)
			return HeartbeatResponse{NeedFullTabletReport: true}, nil
		}
		d.LastReportSeqNo = req.TabletReport.SequenceNo
		d.NeedFullTabletRpt = false
		s.ts.upsert(d)
		if cat != nil {
			_ = cat.ApplyTabletReport(ctx, catalog.TabletReportDelta{
				TSUUID:        req.Instance.PermanentUUID,
				IsIncremental: req.TabletReport.IsIncremental,
				SequenceNo:    req.TabletReport.SequenceNo,
				Updated:       req.TabletReport.Updated,
				RemovedIDs:    req.TabletReport.RemovedIDs,
			})
		}
		return s.buildActionsResponse(cat, planner, req, now, unresponsiveTimeout, reporterWasStale)
	}

	if req.Instance.InstanceSeqNo < d.Instance.InstanceSeqNo {
		return HeartbeatResponse{}, dberrors.New(dberrors.ErrConflict, "stale tserver instance sequence", true, nil)
	}

	resp := HeartbeatResponse{}
	if req.Instance.InstanceSeqNo > d.Instance.InstanceSeqNo {
		d = TSDescriptor{
			Instance:          req.Instance,
			LastHeartbeatAt:   now,
			LastReportSeqNo:   0,
			NeedFullTabletRpt: true,
		}
		if req.Registration != nil {
			d.Registration = *req.Registration
		}
		resp.NeedFullTabletReport = true
		s.ts.upsert(d)
		return resp, nil
	}

	d.LastHeartbeatAt = now
	if req.Registration != nil {
		d.Registration = *req.Registration
	}

	if req.TabletReport.IsIncremental {
		expected := d.LastReportSeqNo + 1
		if d.NeedFullTabletRpt || req.TabletReport.SequenceNo != expected {
			d.NeedFullTabletRpt = true
			resp.NeedFullTabletReport = true
			s.ts.upsert(d)
			return resp, nil
		}
		d.LastReportSeqNo = req.TabletReport.SequenceNo
		d.NeedFullTabletRpt = false
		s.ts.upsert(d)
		if cat != nil {
			_ = cat.ApplyTabletReport(ctx, catalog.TabletReportDelta{
				TSUUID:        req.Instance.PermanentUUID,
				IsIncremental: req.TabletReport.IsIncremental,
				SequenceNo:    req.TabletReport.SequenceNo,
				Updated:       req.TabletReport.Updated,
				RemovedIDs:    req.TabletReport.RemovedIDs,
			})
		}
		return s.buildActionsResponse(cat, planner, req, now, unresponsiveTimeout, reporterWasStale)
	}

	d.LastReportSeqNo = req.TabletReport.SequenceNo
	d.NeedFullTabletRpt = false
	s.ts.upsert(d)
	if cat != nil {
		_ = cat.ApplyTabletReport(ctx, catalog.TabletReportDelta{
			TSUUID:        req.Instance.PermanentUUID,
			IsIncremental: req.TabletReport.IsIncremental,
			SequenceNo:    req.TabletReport.SequenceNo,
			Updated:       req.TabletReport.Updated,
			RemovedIDs:    req.TabletReport.RemovedIDs,
		})
	}
	return s.buildActionsResponse(cat, planner, req, now, unresponsiveTimeout, reporterWasStale)
}

func (s *Service) buildActionsResponse(cat *catalog.Manager, planner *catalog.DirectivePlanner, req HeartbeatRequest, now time.Time, unresponsiveTimeout time.Duration, reporterWasStale bool) (HeartbeatResponse, error) {
	if reporterWasStale {
		return HeartbeatResponse{}, nil
	}
	if cat == nil || planner == nil {
		return HeartbeatResponse{}, nil
	}
	sink := cat.GetMemoryReconcileSink()
	if sink == nil {
		return HeartbeatResponse{}, nil
	}

	staleSet := make(map[string]struct{})
	for _, d := range s.ts.ListStale(unresponsiveTimeout, now) {
		staleSet[d.Instance.PermanentUUID] = struct{}{}
	}

	actionByTablet := make(map[string]TabletAction)
	candidateIDs := append([]string{}, req.TabletReport.Updated...)
	candidateIDs = append(candidateIDs, req.TabletReport.RemovedIDs...)

	for _, tabletID := range candidateIDs {
		view, ok := sink.GetTablet(tabletID)
		if !ok {
			continue
		}
		filteredView := view
		filteredView.Replicas = make(map[string]catalog.TabletReplicaStatus, len(view.Replicas))
		for tsID, rep := range view.Replicas {
			if _, stale := staleSet[tsID]; stale {
				continue
			}
			filteredView.Replicas[tsID] = rep
		}
		if len(filteredView.Replicas) == 0 {
			filteredView.Tombstoned = true
		}
		dirs := planner.PlanForTablet(filteredView)
		for _, d := range dirs {
			actionByTablet[d.TabletID] = TabletAction{TabletID: d.TabletID, Action: string(d.Action)}
		}
	}

	actionIDs := make([]string, 0, len(actionByTablet))
	for id := range actionByTablet {
		actionIDs = append(actionIDs, id)
	}
	sort.Strings(actionIDs)
	actions := make([]TabletAction, 0, len(actionIDs))
	for _, id := range actionIDs {
		actions = append(actions, actionByTablet[id])
	}
	return HeartbeatResponse{TabletActions: actions}, nil
}
