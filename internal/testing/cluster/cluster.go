// Package cluster provides an in-process multi-node test cluster for integration testing.
package cluster

import (
	"context"
	"fmt"
	"os"
	"sync"

	"GoMultiDB/internal/platform"
	rpcpkg "GoMultiDB/internal/rpc"
	"GoMultiDB/internal/server"
	"GoMultiDB/internal/storage/rocks"
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
func (n *NodeHandle) Runtime() *server.Runtime { return n.runtime }

// ClusterHandle provides access to all nodes in the test cluster.
type ClusterHandle struct {
	mu       sync.Mutex
	masters  []*NodeHandle
	tservers []*NodeHandle
	stopped  bool
}

// Master returns the i-th master node handle (0-indexed).
func (c *ClusterHandle) Master(i int) *NodeHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i < 0 || i >= len(c.masters) {
		panic(fmt.Sprintf("master index %d out of range [0, %d)", i, len(c.masters)))
	}
	return c.masters[i]
}

// TServer returns the i-th tserver node handle (0-indexed).
func (c *ClusterHandle) TServer(i int) *NodeHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i < 0 || i >= len(c.tservers) {
		panic(fmt.Sprintf("tserver index %d out of range [0, %d)", i, len(c.tservers)))
	}
	return c.tservers[i]
}

// NumMasters returns the count of master nodes.
func (c *ClusterHandle) NumMasters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.masters)
}

// NumTServers returns the count of tserver nodes.
func (c *ClusterHandle) NumTServers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tservers)
}

// Teardown stops all nodes and removes their temporary data directories.
func (c *ClusterHandle) Teardown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return nil
	}
	c.stopped = true

	ctx := context.Background()
	var firstErr error
	for _, nodes := range [][]*NodeHandle{c.masters, c.tservers} {
		for _, n := range nodes {
			if err := n.runtime.Stop(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
			if n.tempDir != "" {
				_ = os.RemoveAll(n.tempDir)
			}
		}
	}
	return firstErr
}

// StartTestCluster builds and starts an in-process cluster with the given spec.
// All nodes use ephemeral ports and temporary directories. Callers must call
// Teardown() when done to release resources.
func StartTestCluster(ctx context.Context, spec TestClusterSpec) (*ClusterHandle, error) {
	if spec.MemoryLimitBytes <= 0 {
		spec.MemoryLimitBytes = 512 << 20 // 512 MiB
	}
	if spec.ReplicationFactor <= 0 {
		spec.ReplicationFactor = 1
	}

	handle := &ClusterHandle{}

	startNode := func(role string, idx int) (*NodeHandle, error) {
		nodeID := fmt.Sprintf("%s-%d", role, idx)

		tempDir, err := os.MkdirTemp("", "multidb-test-"+nodeID+"-*")
		if err != nil {
			return nil, fmt.Errorf("mktemp for %s: %w", nodeID, err)
		}

		fsm := &platform.FSManager{DataDirs: []string{tempDir}}
		if err := fsm.Init(); err != nil {
			_ = os.RemoveAll(tempDir)
			return nil, fmt.Errorf("fsmanager init for %s: %w", nodeID, err)
		}

		rpcSrv, err := rpcpkg.NewServer(rpcpkg.Config{
			BindAddress:         "127.0.0.1:0",
			StrictContractCheck: false,
		})
		if err != nil {
			_ = os.RemoveAll(tempDir)
			return nil, fmt.Errorf("rpc server for %s: %w", nodeID, err)
		}

		cfg := server.DefaultConfig()
		cfg.NodeID = nodeID
		cfg.RPCBindAddress = "127.0.0.1:0"
		cfg.DataDirs = []string{tempDir}
		cfg.MemoryHardLimitBytes = spec.MemoryLimitBytes
		cfg.EnableSQL = spec.EnableSQL
		cfg.EnableCQL = spec.EnableCQL

		rocksStore := rocks.NewMemoryStore()
		rt, err := server.NewRuntime(cfg, rpcSrv, rocksStore)
		if err != nil {
			_ = os.RemoveAll(tempDir)
			return nil, fmt.Errorf("runtime for %s: %w", nodeID, err)
		}

		if err := rt.Start(ctx); err != nil {
			_ = os.RemoveAll(tempDir)
			return nil, fmt.Errorf("start %s: %w", nodeID, err)
		}

		return &NodeHandle{
			NodeID:    nodeID,
			runtime:   rt,
			rpcServer: rpcSrv,
			tempDir:   tempDir,
			fsm:       fsm,
		}, nil
	}

	for i := 0; i < spec.NumMasters; i++ {
		n, err := startNode("master", i)
		if err != nil {
			_ = handle.Teardown()
			return nil, err
		}
		handle.masters = append(handle.masters, n)
	}

	for i := 0; i < spec.NumTServers; i++ {
		n, err := startNode("tserver", i)
		if err != nil {
			_ = handle.Teardown()
			return nil, err
		}
		handle.tservers = append(handle.tservers, n)
	}

	return handle, nil
}
