// Package snapshot implements the master's distributed snapshot
// coordination: Coordinator owns a SnapshotInfo per snapshot and fans out
// CreateTabletSnapshot, DeleteTabletSnapshot, and RestoreTabletSnapshot
// calls (via the TabletSnapshotRPC interface) to every tablet in the
// snapshot, persisting descriptors through a SnapshotStore
// (RocksSnapshotStore, backed by infra/storage/rocks) so in-progress
// snapshots can be resumed after a restart. Client and RegistryClient
// implement TabletSnapshotRPC over infra/rpc against a
// TabletRPCRegistry-resolved tablet endpoint, and Service exposes the
// coordinator's operations via JSON-RPC. It is consumed by v2/server (the
// master runtime, which wires Coordinator in as its snapshot control
// plane) and v2/server/master/registry (which supplies the
// TabletRPCRegistry).
// This is scaffold-only; behavior is unimplemented.
package snapshot
