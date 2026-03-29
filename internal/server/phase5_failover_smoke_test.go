package server

import (
	"context"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/query/sql"
	rpcpkg "GoMultiDB/internal/rpc"
	"GoMultiDB/internal/storage/rocks"
)

func TestPhase5FailoverRuntimeSQLStatusTransition(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "phase5-failover"
	cfg.EnableSQL = true

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
		t.Fatalf("status before induced failover: %v", err)
	}
	if !status.Healthy {
		t.Fatalf("expected healthy before induced failover")
	}

	s, ok := r.sqlCoord.(*sql.LocalCoordinator)
	if !ok {
		t.Fatalf("expected local coordinator")
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("induce stop as failover simulation: %v", err)
	}

	status, err = r.GetSQLStatus(ctx)
	if err != nil {
		t.Fatalf("status after induced failover: %v", err)
	}
	if status.Healthy {
		t.Fatalf("expected unhealthy status after induced failover")
	}

	n := dberrors.NormalizeError(s.Health(ctx))
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable after induced failover, got %s", n.Code)
	}
}
