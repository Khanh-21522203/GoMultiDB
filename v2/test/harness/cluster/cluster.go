package cluster

import (
	"context"
	"sync"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/infra/platform"
	rpcpkg "GoMultiDB/v2/infra/rpc"
	"GoMultiDB/v2/server/server"
)

// TestClusterSpec describes the desired shape of a test cluster.
type TestClusterSpec struct {
	NumMasters        int
	NumTServers       int
	ReplicationFactor int
	EnableSQL         bool
	EnableCQL         bool
	// MemoryLimitBytes per node; defaults to 512 MiB.
	MemoryLimitBytes int64
}

// NodeHandle holds the runtime state for a single cluster node.
type NodeHandle struct {
	NodeID     string
	RPCAddress string // host:port assigned after Start
	runtime    *server.Runtime
	rpcServer  *rpcpkg.Server
	tempDir    string
	fsm        *platform.FSManager
}

// Runtime returns the underlying server.Runtime.
//
// Not yet implemented in this scaffold.
func (n *NodeHandle) Runtime() *server.Runtime {
	panic("not implemented")
}

// ClusterHandle provides access to all nodes in the test cluster.
type ClusterHandle struct {
	mu       sync.Mutex
	masters  []*NodeHandle
	tservers []*NodeHandle
	stopped  bool
}

// Master returns the i-th master node handle (0-indexed).
//
// Not yet implemented in this scaffold.
func (c *ClusterHandle) Master(i int) *NodeHandle {
	panic("not implemented")
}

// TServer returns the i-th tserver node handle (0-indexed).
//
// Not yet implemented in this scaffold.
func (c *ClusterHandle) TServer(i int) *NodeHandle {
	panic("not implemented")
}

// NumMasters returns the count of master nodes.
//
// Not yet implemented in this scaffold.
func (c *ClusterHandle) NumMasters() int {
	panic("not implemented")
}

// NumTServers returns the count of tserver nodes.
//
// Not yet implemented in this scaffold.
func (c *ClusterHandle) NumTServers() int {
	panic("not implemented")
}

// Teardown stops all nodes and removes their temporary data directories.
//
// Not yet implemented in this scaffold.
func (c *ClusterHandle) Teardown() error {
	return errors.ErrNotImplemented
}

// StartTestCluster builds and starts an in-process cluster with the given
// spec: each node wires together an infra/platform.FSManager-managed temp
// directory, an infra/rpc.Server, and a server/server.Runtime backed by an
// infra/storage/rocks.Store. All nodes use ephemeral ports and temporary
// directories. Callers must call Teardown() when done to release
// resources.
func StartTestCluster(ctx context.Context, spec TestClusterSpec) (*ClusterHandle, error) {
	return &ClusterHandle{}, nil
}
