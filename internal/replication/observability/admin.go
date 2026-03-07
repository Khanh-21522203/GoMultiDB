package observability

import (
	"encoding/json"
	"fmt"
	"net/http"
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
		for _, k := range keys {
			val := snap[k]
			metricName := sanitizeMetricName(k)
			fmt.Fprintf(&b, "%s %v\n", metricName, val)
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

func (r *Registry) PProfHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("pprof placeholder\n"))
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
