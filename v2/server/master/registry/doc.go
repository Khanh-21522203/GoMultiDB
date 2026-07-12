// Package registry provides a tablet RPC endpoint registry backed by the
// master's tablet server manager and tablet placement reconciliation
// state: TabletRPCRegistry resolves a tablet ID to the RPC endpoint of its
// current primary replica by combining a ReconcileSink's placement view
// with a TSManager's tablet server registration data. It is consumed by
// v2/server (the master runtime), which uses it to route tablet-directed
// RPCs such as snapshot operations to the correct tablet server.
// This is scaffold-only; behavior is unimplemented.
package registry
