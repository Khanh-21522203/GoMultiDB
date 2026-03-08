package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sort"
	"strings"
)

func (r *Registry) MetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		snap, err := r.Snapshot(req.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		keys := make([]string, 0, len(snap))
		for k := range snap {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		var b strings.Builder

		// Emit counters and gauges.
		for _, k := range keys {
			val := snap[k]
			metricName := sanitizeMetricName(k)
			fmt.Fprintf(&b, "%s %v\n", metricName, val)
		}

		// Emit histograms in Prometheus text format.
		r.mu.RLock()
		histNames := make([]string, 0, len(r.histograms))
		for name := range r.histograms {
			histNames = append(histNames, name)
		}
		r.mu.RUnlock()
		sort.Strings(histNames)

		for _, name := range histNames {
			buckets, counts, sum, count, ok := r.HistogramSnapshot(name)
			if !ok {
				continue
			}
			desc := ""
			r.mu.RLock()
			if d, found := r.descriptors[name]; found {
				desc = d.Help
			}
			r.mu.RUnlock()

			if desc != "" {
				fmt.Fprintf(&b, "# HELP %s %s\n", name, desc)
			}
			fmt.Fprintf(&b, "# TYPE %s histogram\n", name)
			// Cumulative buckets.
			var cumulative uint64
			for i, upper := range buckets {
				cumulative += counts[i]
				fmt.Fprintf(&b, "%s_bucket{le=\"%g\"} %d\n", name, upper, cumulative)
			}
			// +Inf bucket = total count.
			fmt.Fprintf(&b, "%s_bucket{le=\"+Inf\"} %d\n", name, count)
			fmt.Fprintf(&b, "%s_sum %g\n", name, sum)
			fmt.Fprintf(&b, "%s_count %d\n", name, count)
		}

		_, _ = w.Write([]byte(b.String()))
	}
}

func (r *Registry) VarzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		h, err := r.Healthz(req.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"health": h, "count": len(h)})
	}
}

func (r *Registry) RPCzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		h, err := r.Healthz(req.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		unhealthy := 0
		for _, hs := range h {
			if !hs.Healthy {
				unhealthy++
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"component":          "rpcz",
			"status":             "ok",
			"unhealthy_components": unhealthy,
		})
	}
}

// PProfMux returns an http.ServeMux with all standard pprof routes registered.
// Mount it at /debug/pprof/ in your admin server.
func PProfMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// PProfHandler returns a handler that serves the pprof index page.
// For full pprof support use PProfMux() and register it at /debug/pprof/.
func (r *Registry) PProfHandler() http.HandlerFunc {
	return pprof.Index
}

// DrainHandler returns an HTTP handler for graceful drain initiation.
// It sets the registry's drain flag; callers should poll IsDraining().
func (r *Registry) DrainHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		r.mu.Lock()
		r.draining = true
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"draining": true})
	}
}

// IsDraining returns true if the node is in drain mode.
func (r *Registry) IsDraining() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.draining
}

// TabletStateProvider is the interface the /api/tablets handler uses to list tablets.
type TabletStateProvider interface {
	// TabletStates returns a list of opaque tablet state summaries (any JSON-serialisable value).
	TabletStates() []any
}

// TabletsHandler returns an HTTP handler that lists all tablet states.
func (r *Registry) TabletsHandler(provider TabletStateProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		states := provider.TabletStates()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tablets": states, "count": len(states)})
	}
}

// CompactHandler returns an HTTP handler for triggering manual RocksDB compaction.
// The actual compaction is delegated to the provided compactor function.
func CompactHandler(compact func(ctx context.Context, tabletID string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		tabletID := req.URL.Query().Get("tablet_id")
		if tabletID == "" {
			http.Error(w, "tablet_id query parameter is required", http.StatusBadRequest)
			return
		}
		if err := compact(req.Context(), tabletID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"compacted": tabletID})
	}
}

// CancelHandler returns an HTTP handler for cancelling a live query by request ID.
// The actual cancellation is delegated to the provided cancel function.
func CancelHandler(cancel func(ctx context.Context, requestID string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		requestID := req.URL.Query().Get("request_id")
		if requestID == "" {
			http.Error(w, "request_id query parameter is required", http.StatusBadRequest)
			return
		}
		if err := cancel(req.Context(), requestID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"cancelled": requestID})
	}
}

func sanitizeMetricName(in string) string {
	in = strings.ReplaceAll(in, "{", "_")
	in = strings.ReplaceAll(in, "}", "")
	in = strings.ReplaceAll(in, ",", "_")
	in = strings.ReplaceAll(in, "=", "_")
	in = strings.ReplaceAll(in, ".", "_")
	in = strings.ReplaceAll(in, "-", "_")
	return in
}
