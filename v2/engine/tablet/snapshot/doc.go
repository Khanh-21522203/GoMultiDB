// Package snapshot provides tablet-level point-in-time snapshots: Store
// captures, restores, and lists snapshots of a tablet's data by copying
// key-value pairs out of (and back into) an infra/storage/rocks.Store, and
// Service exposes create/delete/restore operations over infra/rpc for
// remote invocation. It is consumed by v2/server (the tablet server
// runtime, which registers Service) and by v2/replication for
// snapshot-based recovery flows.
// This is scaffold-only; behavior is unimplemented.
package snapshot
