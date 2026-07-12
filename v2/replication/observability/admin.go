package observability

import (
	"context"
	"net/http"
)

// MetricsHandler returns an HTTP handler that renders the registry's
// counters, gauges, and histograms in Prometheus text-exposition format.
//
// Not yet implemented in this scaffold.
func (r *Registry) MetricsHandler() http.HandlerFunc {
	panic("not implemented")
}

// VarzHandler returns an HTTP handler that renders the registry's health
// statuses as a JSON object.
//
// Not yet implemented in this scaffold.
func (r *Registry) VarzHandler() http.HandlerFunc {
	panic("not implemented")
}

// RPCzHandler returns an HTTP handler that renders an RPC-focused summary
// of the registry's health statuses as a JSON object.
//
// Not yet implemented in this scaffold.
func (r *Registry) RPCzHandler() http.HandlerFunc {
	panic("not implemented")
}

// PProfMux returns an http.ServeMux with the standard net/http/pprof
// routes registered. Mount it at /debug/pprof/ in an admin server.
//
// Not yet implemented in this scaffold.
func PProfMux() *http.ServeMux {
	panic("not implemented")
}

// PProfHandler returns a handler that serves the pprof index page. For
// full pprof support, mount PProfMux() at /debug/pprof/ instead.
//
// Not yet implemented in this scaffold.
func (r *Registry) PProfHandler() http.HandlerFunc {
	panic("not implemented")
}

// DrainHandler returns an HTTP handler that sets the registry's drain
// flag on a POST request; callers should poll IsDraining afterward.
//
// Not yet implemented in this scaffold.
func (r *Registry) DrainHandler() http.HandlerFunc {
	panic("not implemented")
}

// IsDraining reports whether the node has been placed into drain mode.
//
// Not yet implemented in this scaffold.
func (r *Registry) IsDraining() bool {
	panic("not implemented")
}

// TabletStateProvider is implemented by callers that can list opaque,
// JSON-serialisable tablet state summaries for the /api/tablets admin
// endpoint.
type TabletStateProvider interface {
	// TabletStates returns a list of opaque tablet state summaries.
	TabletStates() []any
}

// TabletsHandler returns an HTTP handler that lists all tablet states
// reported by provider.
//
// Not yet implemented in this scaffold.
func (r *Registry) TabletsHandler(provider TabletStateProvider) http.HandlerFunc {
	panic("not implemented")
}

// CompactHandler returns an HTTP handler that triggers manual compaction
// of the tablet named by the "tablet_id" query parameter, delegating the
// actual compaction to compact.
//
// Not yet implemented in this scaffold.
func CompactHandler(compact func(ctx context.Context, tabletID string) error) http.HandlerFunc {
	panic("not implemented")
}

// CancelHandler returns an HTTP handler that cancels the live query named
// by the "request_id" query parameter, delegating the actual cancellation
// to cancel.
//
// Not yet implemented in this scaffold.
func CancelHandler(cancel func(ctx context.Context, requestID string) error) http.HandlerFunc {
	panic("not implemented")
}
