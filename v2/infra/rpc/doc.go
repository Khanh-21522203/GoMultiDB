// Package rpc provides an HTTP-based JSON-RPC client and server used for
// node-to-node communication in GoMultiDB v2: Client issues typed calls
// against a peer's Server, which dispatches requests to registered Service
// implementations by service and method name, each wrapped in a
// contracts/types.RequestEnvelope. It underlies the v2/server node
// runtimes (master and tablet server), v2/server/services/ping,
// v2/server/master/heartbeat, v2/server/master/snapshot,
// v2/engine/tablet/snapshot, and v2/replication/cdc for their
// control-plane and data-plane RPCs.
// This is scaffold-only; behavior is unimplemented.
package rpc
