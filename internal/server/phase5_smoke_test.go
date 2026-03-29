package server

import (
	"context"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/query/cql"
	"GoMultiDB/internal/query/sql"
	rpcpkg "GoMultiDB/internal/rpc"
	"GoMultiDB/internal/storage/rocks"
)

func TestPhase5SmokeRuntimeQueryStatus(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "phase5-smoke"
	cfg.EnableSQL = true
	cfg.EnableCQL = true
	cfg.CQLMaxConnections = 4

	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{BindAddress: "127.0.0.1:0", StrictContractCheck: true})
	if err != nil {
		t.Fatalf("new rpc server: %v", err)
	}

	rocksStore := rocks.NewMemoryStore()
	r, err := NewRuntime(cfg, rpcServer, rocksStore)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := r.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		_ = r.Stop(context.Background())
	}()

	status, err := r.GetSQLStatus(ctx)
	if err != nil {
		t.Fatalf("get sql status: %v", err)
	}
	if !status.Enabled {
		t.Fatalf("expected sql enabled")
	}
	if !status.Healthy {
		t.Fatalf("expected sql healthy")
	}

	c, ok := r.cqlServer.(*cql.LocalServer)
	if !ok {
		t.Fatalf("expected local cql server type")
	}
	if _, err := c.Route(ctx, cql.Request{ConnID: "phase5-smoke-conn", Query: "SELECT now()"}); err != nil {
		t.Fatalf("cql route from runtime: %v", err)
	}

	s := r.sqlCoord.(*sql.LocalCoordinator)
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop sql coordinator: %v", err)
	}

	status, err = r.GetSQLStatus(ctx)
	if err != nil {
		t.Fatalf("get sql status after stop: %v", err)
	}
	if status.Healthy {
		t.Fatalf("expected sql unhealthy after stop")
	}

	n := dberrors.NormalizeError(s.Health(ctx))
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable health code after stop, got %s", n.Code)
	}
}
