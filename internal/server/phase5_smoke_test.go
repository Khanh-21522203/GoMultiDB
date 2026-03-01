package server

import (
	"context"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/query/ycql"
	"GoMultiDB/internal/query/ysql"
	rpcpkg "GoMultiDB/internal/rpc"
)

func TestPhase5SmokeRuntimeQueryStatus(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "phase5-smoke"
	cfg.EnableYSQL = true
	cfg.EnableYCQL = true
	cfg.YCQLMaxConnections = 4

	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{BindAddress: "127.0.0.1:0", StrictContractCheck: true})
	if err != nil {
		t.Fatalf("new rpc server: %v", err)
	}

	r, err := NewRuntime(cfg, rpcServer)
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

	status, err := r.GetYSQLStatus(ctx)
	if err != nil {
		t.Fatalf("get ysql status: %v", err)
	}
	if !status.Enabled {
		t.Fatalf("expected ysql enabled")
	}
	if !status.Healthy {
		t.Fatalf("expected ysql healthy")
	}

	y, ok := r.ycql.(*ycql.LocalServer)
	if !ok {
		t.Fatalf("expected local ycql server type")
	}
	if _, err := y.Route(ctx, ycql.Request{ConnID: "phase5-smoke-conn", Query: "SELECT now()"}); err != nil {
		t.Fatalf("ycql route from runtime: %v", err)
	}

	ys := r.ysql.(*ysql.LocalCoordinator)
	if err := ys.Stop(ctx); err != nil {
		t.Fatalf("stop ysql coordinator: %v", err)
	}

	status, err = r.GetYSQLStatus(ctx)
	if err != nil {
		t.Fatalf("get ysql status after stop: %v", err)
	}
	if status.Healthy {
		t.Fatalf("expected ysql unhealthy after stop")
	}

	n := dberrors.NormalizeError(ys.Health(ctx))
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable health code after stop, got %s", n.Code)
	}
}
