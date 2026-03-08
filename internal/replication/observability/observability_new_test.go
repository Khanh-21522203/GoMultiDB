package observability_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"GoMultiDB/internal/replication/observability"
)

// ── pprof ─────────────────────────────────────────────────────────────────────

func TestPProfIndexServed(t *testing.T) {
	mux := observability.PProfMux()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "goroutine") {
		t.Fatalf("pprof index should mention goroutine, body: %s", body[:min(200, len(body))])
	}
}

func TestPProfHandlerIsNotPlaceholder(t *testing.T) {
	r := observability.NewRegistry()
	handler := r.PProfHandler()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rw := httptest.NewRecorder()
	handler(rw, req)

	body, _ := io.ReadAll(rw.Result().Body)
	if strings.Contains(string(body), "placeholder") {
		t.Fatalf("PProfHandler should not return placeholder content")
	}
}

// ── histogram ─────────────────────────────────────────────────────────────────

func TestHistogramObserveAndSnapshot(t *testing.T) {
	r := observability.NewRegistry()
	ctx := context.Background()

	err := r.RegisterHistogram(ctx, "latency_ms", "Request latency in ms", []float64{1, 5, 10, 50, 100})
	if err != nil {
		t.Fatalf("RegisterHistogram: %v", err)
	}

	observations := []float64{0.5, 3, 8, 12, 200}
	for _, v := range observations {
		if err := r.ObserveHistogram(ctx, "latency_ms", v); err != nil {
			t.Fatalf("ObserveHistogram(%g): %v", v, err)
		}
	}

	buckets, counts, sum, count, ok := r.HistogramSnapshot("latency_ms")
	if !ok {
		t.Fatalf("histogram snapshot not found")
	}
	if count != uint64(len(observations)) {
		t.Fatalf("count: want %d, got %d", len(observations), count)
	}
	if sum != 0.5+3+8+12+200 {
		t.Fatalf("sum: want %g, got %g", 0.5+3+8+12+200, sum)
	}
	// Bucket at le=1 should include 0.5 → count=1.
	if counts[0] != 1 {
		t.Fatalf("bucket[le=1]: want 1, got %d", counts[0])
	}
	// Bucket at le=50 should include 0.5, 3, 8, 12 → 4.
	// buckets: [1,5,10,50,100], index 3 = 50
	if counts[3] != 4 {
		t.Fatalf("bucket[le=50]: want 4, got %d; buckets=%v counts=%v", counts[3], buckets, counts)
	}
	// Bucket at le=100 should include all non-200 observations → 4.
	if counts[4] != 4 {
		t.Fatalf("bucket[le=100]: want 4, got %d", counts[4])
	}
}

func TestHistogramIdempotentRegister(t *testing.T) {
	r := observability.NewRegistry()
	ctx := context.Background()
	if err := r.RegisterHistogram(ctx, "h", "h", nil); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := r.RegisterHistogram(ctx, "h", "h", nil); err != nil {
		t.Fatalf("idempotent register: %v", err)
	}
}

func TestHistogramNotFound(t *testing.T) {
	r := observability.NewRegistry()
	if err := r.ObserveHistogram(context.Background(), "nonexistent", 1.0); err == nil {
		t.Fatalf("expected error for unregistered histogram")
	}
}

// ── MetricsHandler with histogram ─────────────────────────────────────────────

func TestMetricsHandlerPrometheusFormat(t *testing.T) {
	r := observability.NewRegistry()
	ctx := context.Background()

	_ = r.RegisterHistogram(ctx, "rpc_latency_ms", "RPC latency", []float64{1, 5, 10})
	_ = r.ObserveHistogram(ctx, "rpc_latency_ms", 2.5)
	_ = r.ObserveHistogram(ctx, "rpc_latency_ms", 8.0)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rw := httptest.NewRecorder()
	r.MetricsHandler()(rw, req)

	body, _ := io.ReadAll(rw.Result().Body)
	s := string(body)

	if !strings.Contains(s, "# TYPE rpc_latency_ms histogram") {
		t.Fatalf("expected histogram TYPE line, body: %s", s)
	}
	if !strings.Contains(s, `rpc_latency_ms_bucket{le="5"} 1`) {
		// 2.5 is <= 5 → cumulative count at le=5 should be 1 (only 2.5)
		// Actually 2.5 > 1 and 2.5 <= 5, so le=1 count=0, le=5 count=1.
		t.Fatalf("expected bucket{le=5} 1 in output, body: %s", s)
	}
	if !strings.Contains(s, "rpc_latency_ms_count 2") {
		t.Fatalf("expected count=2, body: %s", s)
	}
}

// ── structured logging ────────────────────────────────────────────────────────

func TestSetLogContext(t *testing.T) {
	r := observability.NewRegistry()
	r.SetLogContext(observability.LogContext{
		NodeID:    "node-1",
		TabletID:  "tablet-a",
		TraceID:   "trace-xyz",
		RequestID: "req-001",
	})
	// Just verify no panic and the call is accepted.
	r.Log(slog.LevelInfo, "test log message", "key", "value")
}

// ── drain endpoint ────────────────────────────────────────────────────────────

func TestDrainHandler(t *testing.T) {
	r := observability.NewRegistry()
	if r.IsDraining() {
		t.Fatalf("should not be draining initially")
	}

	req := httptest.NewRequest(http.MethodPost, "/api/drain", nil)
	rw := httptest.NewRecorder()
	r.DrainHandler()(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	if !r.IsDraining() {
		t.Fatalf("expected draining after POST /api/drain")
	}
}

func TestDrainHandlerRejectsGet(t *testing.T) {
	r := observability.NewRegistry()
	req := httptest.NewRequest(http.MethodGet, "/api/drain", nil)
	rw := httptest.NewRecorder()
	r.DrainHandler()(rw, req)
	if rw.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rw.Code)
	}
}

// ── tablets endpoint ──────────────────────────────────────────────────────────

type staticProvider struct{ states []any }

func (s *staticProvider) TabletStates() []any { return s.states }

func TestTabletsHandler(t *testing.T) {
	r := observability.NewRegistry()
	provider := &staticProvider{states: []any{"tab-1", "tab-2"}}
	req := httptest.NewRequest(http.MethodGet, "/api/tablets", nil)
	rw := httptest.NewRecorder()
	r.TabletsHandler(provider)(rw, req)

	body, _ := io.ReadAll(rw.Result().Body)
	if !strings.Contains(string(body), "tab-1") {
		t.Fatalf("expected tab-1 in response, got: %s", body)
	}
}

// ── compact endpoint ──────────────────────────────────────────────────────────

func TestCompactHandler(t *testing.T) {
	compacted := ""
	handler := observability.CompactHandler(func(_ context.Context, tabletID string) error {
		compacted = tabletID
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/api/compact?tablet_id=tab-x", nil)
	rw := httptest.NewRecorder()
	handler(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	if compacted != "tab-x" {
		t.Fatalf("expected compact called with tab-x, got %q", compacted)
	}
}

func TestCompactHandlerMissingTabletID(t *testing.T) {
	handler := observability.CompactHandler(func(_ context.Context, _ string) error { return nil })
	req := httptest.NewRequest(http.MethodPost, "/api/compact", nil)
	rw := httptest.NewRecorder()
	handler(rw, req)
	if rw.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rw.Code)
	}
}

// ── cancel endpoint ───────────────────────────────────────────────────────────

func TestCancelHandler(t *testing.T) {
	cancelled := ""
	handler := observability.CancelHandler(func(_ context.Context, reqID string) error {
		cancelled = reqID
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/api/cancel?request_id=req-42", nil)
	rw := httptest.NewRecorder()
	handler(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	if cancelled != "req-42" {
		t.Fatalf("expected cancel called with req-42, got %q", cancelled)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
