package server

import (
	"context"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/query/ysql"
	rpcpkg "GoMultiDB/internal/rpc"
)

func TestPhase5FailoverRuntimeYSQLStatusTransition(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "phase5-failover"
	cfg.EnableYSQL = true

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
		t.Fatalf("status before induced failover: %v", err)
	}
	if !status.Healthy {
		t.Fatalf("expected healthy before induced failover")
	}

	y, ok := r.ysql.(*ysql.LocalCoordinator)
	if !ok {
		t.Fatalf("expected local coordinator")
	}
	if err := y.Stop(ctx); err != nil {
		t.Fatalf("induce stop as failover simulation: %v", err)
	}

	status, err = r.GetYSQLStatus(ctx)
	if err != nil {
		t.Fatalf("status after induced failover: %v", err)
	}
	if status.Healthy {
		t.Fatalf("expected unhealthy status after induced failover")
	}

	n := dberrors.NormalizeError(y.Health(ctx))
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable after induced failover, got %s", n.Code)
	}
}
