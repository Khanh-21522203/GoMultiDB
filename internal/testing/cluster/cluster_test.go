package cluster_test

import (
	"context"
	"testing"
	"time"

	"GoMultiDB/internal/testing/cluster"
)

func TestStartTestClusterSingleMaster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := cluster.StartTestCluster(ctx, cluster.TestClusterSpec{
		NumMasters:  1,
		NumTServers: 0,
	})
	if err != nil {
		t.Fatalf("StartTestCluster: %v", err)
	}
	defer func() {
		if err := c.Teardown(); err != nil {
			t.Errorf("Teardown: %v", err)
		}
	}()

	if c.NumMasters() != 1 {
		t.Fatalf("want 1 master, got %d", c.NumMasters())
	}
}

func TestStartTestClusterMultipleNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := cluster.StartTestCluster(ctx, cluster.TestClusterSpec{
		NumMasters:  3,
		NumTServers: 2,
	})
	if err != nil {
		t.Fatalf("StartTestCluster: %v", err)
	}
	defer c.Teardown()

	if c.NumMasters() != 3 {
		t.Fatalf("want 3 masters, got %d", c.NumMasters())
	}
	if c.NumTServers() != 2 {
		t.Fatalf("want 2 tservers, got %d", c.NumTServers())
	}
}

func TestStartTestClusterNodeIDsUnique(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := cluster.StartTestCluster(ctx, cluster.TestClusterSpec{
		NumMasters:  2,
		NumTServers: 2,
	})
	if err != nil {
		t.Fatalf("StartTestCluster: %v", err)
	}
	defer c.Teardown()

	seen := make(map[string]bool)
	for i := 0; i < c.NumMasters(); i++ {
		id := c.Master(i).NodeID
		if seen[id] {
			t.Fatalf("duplicate NodeID: %s", id)
		}
		seen[id] = true
	}
	for i := 0; i < c.NumTServers(); i++ {
		id := c.TServer(i).NodeID
		if seen[id] {
			t.Fatalf("duplicate NodeID: %s", id)
		}
		seen[id] = true
	}
}

func TestStartTestClusterTeardownIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := cluster.StartTestCluster(ctx, cluster.TestClusterSpec{NumMasters: 1})
	if err != nil {
		t.Fatalf("StartTestCluster: %v", err)
	}
	if err := c.Teardown(); err != nil {
		t.Fatalf("first Teardown: %v", err)
	}
	// Second teardown must not panic or error.
	if err := c.Teardown(); err != nil {
		t.Fatalf("second Teardown (idempotent): %v", err)
	}
}

func TestStartTestClusterRuntimeNotNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := cluster.StartTestCluster(ctx, cluster.TestClusterSpec{NumMasters: 1, NumTServers: 1})
	if err != nil {
		t.Fatalf("StartTestCluster: %v", err)
	}
	defer c.Teardown()

	if c.Master(0).Runtime() == nil {
		t.Fatalf("master runtime is nil")
	}
	if c.TServer(0).Runtime() == nil {
		t.Fatalf("tserver runtime is nil")
	}
}

func TestStartTestClusterMasterIndexPanicsOutOfRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := cluster.StartTestCluster(ctx, cluster.TestClusterSpec{NumMasters: 1})
	if err != nil {
		t.Fatalf("StartTestCluster: %v", err)
	}
	defer c.Teardown()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for out-of-range master index")
		}
	}()
	_ = c.Master(5)
}
