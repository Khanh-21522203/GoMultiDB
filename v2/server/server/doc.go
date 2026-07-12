// Package server implements the GoMultiDB v2 node runtime: Config
// describes a node's identity, network bindings, storage directories, and
// query-gateway options, and Runtime wires together an infra/rpc.Server,
// the gateway/query/sql and gateway/query/cql coordinators, and (when
// enabled) a server/master/snapshot.Coordinator backed by an
// infra/storage/rocks.Store, driving their combined start-up and
// phased graceful shutdown. It is the top-level assembly point for both
// the master and tablet server binaries.
// This is scaffold-only; behavior is unimplemented.
package server
