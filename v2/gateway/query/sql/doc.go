// Package sql implements a Postgres-wire-compatible query gateway on top
// of GoMultiDB v2: Coordinator (LocalCoordinator, or ManagedCoordinator
// wrapping a real PGProcess subprocess with fallback to LocalCoordinator)
// owns the lifecycle of the SQL front end, and CatalogVersion tracks the
// catalog generation used to invalidate cached query plans. It is consumed
// by v2/server, which exposes it alongside the other query-gateway
// protocols as a tablet-server-facing entry point.
// This is scaffold-only; behavior is unimplemented.
package sql
