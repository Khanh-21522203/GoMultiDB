// Package cql implements a Cassandra-compatible query gateway on top of
// GoMultiDB v2: Listener accepts TCP connections and speaks the CQL binary
// protocol (Frame/Codec/Connection), LocalServer routes parsed requests to
// a SessionManager (per-connection state and prepared-statement cache) and
// executes them, and the wire message types (StartupRequest, QueryRequest,
// ExecuteRequest, BatchRequest, ResultResponse, ...) marshal and unmarshal
// the CQL v4 frame body format. It is consumed by v2/server, which exposes
// it alongside the other query-gateway protocols as a tablet-server-facing
// entry point.
// This is scaffold-only; behavior is unimplemented.
package cql
