package server

import (
	"context"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/master/balancer"
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

	if err := r.sqlCoord.Stop(ctx); err != nil {
		t.Fatalf("induce stop as failover simulation: %v", err)
	}

	status, err = r.GetSQLStatus(ctx)
	if err != nil {
		t.Fatalf("status after induced failover: %v", err)
	}
	if status.Healthy {
		t.Fatalf("expected unhealthy status after induced failover")
	}

	n := dberrors.NormalizeError(r.sqlCoord.Health(ctx))
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable after induced failover, got %s", n.Code)
	}
}

func TestPhase5PrimaryOwnershipTransferRecovery(t *testing.T) {
	state := balancer.ClusterState{
		Nodes: []balancer.NodeLoad{
			{NodeID: "n1", ReplicaCount: 2, LeaderCount: 2, IsLive: true, Placement: "zone-a"},
			{NodeID: "n2", ReplicaCount: 2, LeaderCount: 0, IsLive: true, Placement: "zone-b"},
		},
		Tablets: []balancer.TabletPlacement{
			{
				TabletID: "tab-phase5",
				RF:       2,
				Replicas: []balancer.ReplicaPlacement{
					{NodeID: "n1", IsLeader: true},
					{NodeID: "n2", IsLeader: false},
				},
			},
		},
	}
	p := balancer.NewPlanner(balancer.Config{PrimaryBalancingEnabled: true, CooldownWindow: 0})
	actions, err := p.PlanBalanceRound(state)
	if err != nil {
		t.Fatalf("plan ownership transfer: %v", err)
	}
	if len(actions) != 1 || actions[0].Type != "transfer_primary" {
		t.Fatalf("expected one transfer_primary action, got %+v", actions)
	}

	cfg := DefaultConfig()
	cfg.NodeID = "phase5-primary-recovery"
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
	defer func() { _ = r.Stop(context.Background()) }()

	if err := r.sqlCoord.Stop(ctx); err != nil {
		t.Fatalf("induce ownership transfer restart stop: %v", err)
	}

	before, err := r.GetSQLStatus(ctx)
	if err != nil {
		t.Fatalf("status before recovery: %v", err)
	}
	if before.Healthy {
		t.Fatalf("expected unhealthy SQL before recovery restart")
	}

	if err := r.StartSQL(ctx); err != nil {
		t.Fatalf("restart sql after ownership transfer: %v", err)
	}
	after, err := r.GetSQLStatus(ctx)
	if err != nil {
		t.Fatalf("status after recovery: %v", err)
	}
	if !after.Healthy {
		t.Fatalf("expected healthy SQL after ownership transfer restart")
	}
}
