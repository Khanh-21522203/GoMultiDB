package observability

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestRegistryMetricsHealthAndHandlers(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	if err := r.RegisterMetric(ctx, MetricDescriptor{Name: "replication_apply_total", Type: "counter", Help: "apply count"}); err != nil {
		t.Fatalf("register metric: %v", err)
	}
	if err := r.RecordMetric(ctx, "replication_apply_total", 3, "stream=s1"); err != nil {
		t.Fatalf("record metric: %v", err)
	}
	if err := r.SetHealth(ctx, HealthStatus{Component: "cdc", Healthy: true, Details: "ok"}); err != nil {
		t.Fatalf("set health: %v", err)
	}

	snap, err := r.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap) != 1 {
		t.Fatalf("expected one metric sample, got %d", len(snap))
	}

	h, err := r.Healthz(ctx)
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	if len(h) != 1 || h[0].Component != "cdc" {
		t.Fatalf("unexpected health status: %+v", h)
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	rr := httptest.NewRecorder()
	r.MetricsHandler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("metrics handler status: %d", rr.Code)
	}
	if rr.Body.String() == "" {
		t.Fatalf("expected non-empty metrics body")
	}

	req = httptest.NewRequest("GET", "/varz", nil)
	rr = httptest.NewRecorder()
	r.VarzHandler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("varz handler status: %d", rr.Code)
	}

	req = httptest.NewRequest("GET", "/rpcz", nil)
	rr = httptest.NewRecorder()
	r.RPCzHandler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("rpcz handler status: %d", rr.Code)
	}

	req = httptest.NewRequest("GET", "/pprof", nil)
	rr = httptest.NewRecorder()
	r.PProfHandler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("pprof handler status: %d", rr.Code)
	}
}

func TestSnapshotCardinalityStability(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	if err := r.RegisterMetric(ctx, MetricDescriptor{Name: "replication_apply_total", Type: "counter", Help: "apply count"}); err != nil {
		t.Fatalf("register metric: %v", err)
	}
	for i := 0; i < 100; i++ {
		label := "stream=s1"
		if i%2 == 1 {
			label = "stream=s2"
		}
		if err := r.RecordMetric(ctx, "replication_apply_total", float64(i), label); err != nil {
			t.Fatalf("record metric %d: %v", i, err)
		}
	}
	snap, err := r.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap) != 2 {
		t.Fatalf("expected stable cardinality of 2 label sets, got %d", len(snap))
	}
}

func TestRecordPrimaryOwnershipTransition(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	if err := r.RecordPrimaryOwnershipTransition(ctx, "s-own", "t-own", "ts-1", "ts-2", 4); err != nil {
		t.Fatalf("record primary ownership transition: %v", err)
	}
	snap, err := r.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snap) != 1 {
		t.Fatalf("expected one ownership metric sample, got %d", len(snap))
	}

	health, err := r.Healthz(ctx)
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	found := false
	for _, hs := range health {
		if hs.Component == "primary_ownership" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected primary_ownership health component")
	}
}
