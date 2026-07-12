// Package cluster provides an in-process multi-node test cluster for
// integration testing: StartTestCluster wires together a
// server/server.Runtime per node — each backed by its own infra/rpc.Server,
// an infra/platform.FSManager-managed temporary directory, and an
// infra/storage/rocks.Store — and ClusterHandle exposes the resulting
// master and tablet-server NodeHandles along with combined teardown. It is
// consumed by v2/test/integration and v2/test/stress for tests that need a
// running cluster.
// This is scaffold-only; behavior is unimplemented.
package cluster
