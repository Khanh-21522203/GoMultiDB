package heartbeat

import (
	"context"
	"testing"
	"time"

	"GoMultiDB/internal/master/catalog"
)

func TestHeartbeatRequiresLeader(t *testing.T) {
	svc := NewService(nil, nil)
	_, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance: TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
	})
	if err == nil {
		t.Fatalf("expected not leader error")
	}
}

func TestHeartbeatNeedsReregisterForUnknownWithoutRegistration(t *testing.T) {
	svc := NewService(nil, nil)
	svc.SetLeader(true)

	resp, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance: TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !resp.NeedReregister {
		t.Fatalf("expected need reregister")
	}
}

func TestHeartbeatRegistersAndAcceptsFullReport(t *testing.T) {
	svc := NewService(nil, nil)
	svc.SetLeader(true)

	resp, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
		Registration: &TSRegistration{RPCAddress: "127.0.0.1:9100", HTTPAddress: "127.0.0.1:9000"},
		TabletReport: TabletReport{IsIncremental: false, SequenceNo: 10},
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if resp.NeedFullTabletReport || resp.NeedReregister {
		t.Fatalf("unexpected response flags: %+v", resp)
	}

	d, ok := svc.ts.Get("ts-1")
	if !ok {
		t.Fatalf("expected descriptor to be registered")
	}
	if d.LastReportSeqNo != 10 {
		t.Fatalf("expected report seq=10, got=%d", d.LastReportSeqNo)
	}
}

func TestHeartbeatIncrementalGapForcesFullReport(t *testing.T) {
	svc := NewService(nil, nil)
	svc.SetLeader(true)

	_, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
		Registration: &TSRegistration{RPCAddress: "127.0.0.1:9100", HTTPAddress: "127.0.0.1:9000"},
		TabletReport: TabletReport{IsIncremental: false, SequenceNo: 5},
	})
	if err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	resp, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
		TabletReport: TabletReport{IsIncremental: true, SequenceNo: 7},
	})
	if err != nil {
		t.Fatalf("incremental heartbeat: %v", err)
	}
	if !resp.NeedFullTabletReport {
		t.Fatalf("expected full report request due to sequence gap")
	}
}

func TestHeartbeatRestartDetectionForcesFullReport(t *testing.T) {
	svc := NewService(nil, nil)
	svc.SetLeader(true)

	_, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
		Registration: &TSRegistration{RPCAddress: "127.0.0.1:9100", HTTPAddress: "127.0.0.1:9000"},
		TabletReport: TabletReport{IsIncremental: false, SequenceNo: 5},
	})
	if err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	resp, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 2},
		Registration: &TSRegistration{RPCAddress: "127.0.0.1:9100", HTTPAddress: "127.0.0.1:9000"},
		TabletReport: TabletReport{IsIncremental: true, SequenceNo: 1},
	})
	if err != nil {
		t.Fatalf("restart heartbeat: %v", err)
	}
	if !resp.NeedFullTabletReport {
		t.Fatalf("expected full report after restart detection")
	}
}

func TestHeartbeatRejectsStaleInstanceSequence(t *testing.T) {
	svc := NewService(nil, nil)
	svc.SetLeader(true)

	_, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 3},
		Registration: &TSRegistration{RPCAddress: "127.0.0.1:9100", HTTPAddress: "127.0.0.1:9000"},
		TabletReport: TabletReport{IsIncremental: false, SequenceNo: 1},
	})
	if err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	_, err = svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance: TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 2},
	})
	if err == nil {
		t.Fatalf("expected stale instance sequence error")
	}
}

type testReconcileSink struct {
	got catalog.TabletReportDelta
}

func (s *testReconcileSink) ApplyTabletReport(_ context.Context, delta catalog.TabletReportDelta) error {
	s.got = delta
	return nil
}

func TestHeartbeatDelegatesReportToCatalogSink(t *testing.T) {
	cat, err := catalog.NewManager(catalog.NewMemoryStore())
	if err != nil {
		t.Fatalf("new catalog manager: %v", err)
	}
	sink := &testReconcileSink{}
	cat.SetReconcileSink(sink)

	svc := NewService(nil, cat)
	svc.SetLeader(true)

	_, err = svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
		Registration: &TSRegistration{RPCAddress: "127.0.0.1:9100", HTTPAddress: "127.0.0.1:9000"},
		TabletReport: TabletReport{IsIncremental: false, SequenceNo: 11, Updated: []string{"tablet-a"}},
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if sink.got.TSUUID != "ts-1" || sink.got.SequenceNo != 11 {
		t.Fatalf("unexpected sink delta: %+v", sink.got)
	}
}

func TestHeartbeatReturnsTabletActionsFromPlanner(t *testing.T) {
	cat, err := catalog.NewManager(catalog.NewMemoryStore())
	if err != nil {
		t.Fatalf("new catalog manager: %v", err)
	}
	memSink := catalog.NewMemoryReconcileSink()
	cat.SetReconcileSink(memSink)

	svc := NewService(nil, cat)
	svc.SetLeader(true)

	resp, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
		Registration: &TSRegistration{RPCAddress: "127.0.0.1:9100", HTTPAddress: "127.0.0.1:9000"},
		TabletReport: TabletReport{IsIncremental: false, SequenceNo: 1, Updated: []string{"tablet-a"}},
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if len(resp.TabletActions) == 0 {
		t.Fatalf("expected at least one tablet action")
	}
	if resp.TabletActions[0].Action != string(catalog.DirectiveCreateTablet) {
		t.Fatalf("expected create-tablet action, got %+v", resp.TabletActions[0])
	}
}

func TestTSManagerListStale(t *testing.T) {
	m := NewTSManager()
	now := time.Unix(1000, 0).UTC()
	m.upsert(TSDescriptor{
		Instance:        TSInstance{PermanentUUID: "ts-stale", InstanceSeqNo: 1},
		LastHeartbeatAt: now.Add(-2 * time.Minute),
	})
	m.upsert(TSDescriptor{
		Instance:        TSInstance{PermanentUUID: "ts-fresh", InstanceSeqNo: 1},
		LastHeartbeatAt: now.Add(-10 * time.Second),
	})

	stale := m.ListStale(1*time.Minute, now)
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale tserver, got %d", len(stale))
	}
	if stale[0].Instance.PermanentUUID != "ts-stale" {
		t.Fatalf("unexpected stale tserver: %s", stale[0].Instance.PermanentUUID)
	}
}

func TestHeartbeatPlannerSkipsStaleReplicas(t *testing.T) {
	cat, err := catalog.NewManager(catalog.NewMemoryStore())
	if err != nil {
		t.Fatalf("new catalog manager: %v", err)
	}
	memSink := catalog.NewMemoryReconcileSink()
	cat.SetReconcileSink(memSink)

	svc := NewService(NewTSManager(), cat)
	svc.SetLeader(true)
	now := time.Unix(1000, 0).UTC()
	svc.nowFn = func() time.Time { return now }
	svc.unresponsiveTimeout = 30 * time.Second

	svc.ts.upsert(TSDescriptor{Instance: TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1}, LastHeartbeatAt: now})
	svc.ts.upsert(TSDescriptor{Instance: TSInstance{PermanentUUID: "ts-2", InstanceSeqNo: 1}, LastHeartbeatAt: now.Add(-2 * time.Minute)})

	if err := memSink.ApplyTabletReport(context.Background(), catalog.TabletReportDelta{TSUUID: "ts-1", SequenceNo: 1, Updated: []string{"tablet-a"}}); err != nil {
		t.Fatalf("apply sink ts-1: %v", err)
	}
	if err := memSink.ApplyTabletReport(context.Background(), catalog.TabletReportDelta{TSUUID: "ts-2", SequenceNo: 1, Updated: []string{"tablet-a"}}); err != nil {
		t.Fatalf("apply sink ts-2: %v", err)
	}

	resp, err := svc.TSHeartbeat(context.Background(), HeartbeatRequest{
		Instance:     TSInstance{PermanentUUID: "ts-1", InstanceSeqNo: 1},
		TabletReport: TabletReport{IsIncremental: false, SequenceNo: 2, Updated: []string{"tablet-a"}},
	})
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if len(resp.TabletActions) == 0 {
		t.Fatalf("expected action when stale replica is excluded and tablet becomes under-replicated")
	}
}
