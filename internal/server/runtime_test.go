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

type ysqlStub struct {
	startCalls int
	stopCalls  int
	healthErr  error
}

func (s *ysqlStub) Start(_ context.Context, _ ysql.ProcessConfig) error {
	s.startCalls++
	s.healthErr = nil
	return nil
}

func (s *ysqlStub) Stop(_ context.Context) error {
	s.stopCalls++
	s.healthErr = dberrors.New(dberrors.ErrRetryableUnavailable, "ysql is not started", true, nil)
	return nil
}

func (s *ysqlStub) Health(_ context.Context) error {
	return s.healthErr
}

type ycqlStub struct {
	startCalls int
	stopCalls  int
	healthErr  error
}

func (s *ycqlStub) Start(_ context.Context, _ ycql.Config) error {
	s.startCalls++
	return nil
}

func (s *ycqlStub) Stop(_ context.Context) error {
	s.stopCalls++
	return nil
}

func (s *ycqlStub) Health(_ context.Context) error {
	return s.healthErr
}

func (s *ycqlStub) Route(_ context.Context, _ ycql.Request) (ycql.Response, error) {
	if s.healthErr != nil {
		return ycql.Response{}, s.healthErr
	}
	return ycql.Response{Applied: true}, nil
}

func (s *ycqlStub) RouteBatch(_ context.Context, _ ycql.BatchRequest) (ycql.Response, error) {
	if s.healthErr != nil {
		return ycql.Response{}, s.healthErr
	}
	return ycql.Response{Applied: true}, nil
}

func makeRuntimeForTest(t *testing.T, cfg Config) *Runtime {
	t.Helper()
	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{BindAddress: "127.0.0.1:0", StrictContractCheck: true})
	if err != nil {
		t.Fatalf("new rpc server: %v", err)
	}
	r, err := NewRuntime(cfg, rpcServer)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return r
}

func TestRuntimeStartsAndStopsQueryCoordinators(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "tserver-test"
	r := makeRuntimeForTest(t, cfg)

	ysqlS := &ysqlStub{}
	ycqlS := &ycqlStub{}
	r.ysql = ysqlS
	r.ycql = ycqlS

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	if ysqlS.startCalls != 1 {
		t.Fatalf("expected one ysql start call, got %d", ysqlS.startCalls)
	}
	if ycqlS.startCalls != 1 {
		t.Fatalf("expected one ycql start call, got %d", ycqlS.startCalls)
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("stop runtime: %v", err)
	}
	if ysqlS.stopCalls != 1 {
		t.Fatalf("expected one ysql stop call, got %d", ysqlS.stopCalls)
	}
	if ycqlS.stopCalls != 1 {
		t.Fatalf("expected one ycql stop call, got %d", ycqlS.stopCalls)
	}
}

func TestLocalYSQLCoordinatorHealthRetryableWhenStopped(t *testing.T) {
	c := ysql.NewLocalCoordinator()
	err := c.Health(context.Background())
	if err == nil {
		t.Fatalf("expected health error when ysql is stopped")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable, got %s", n.Code)
	}
}

func TestLocalYCQLServerHealthRetryableWhenStopped(t *testing.T) {
	s := ycql.NewLocalServer()
	err := s.Health(context.Background())
	if err == nil {
		t.Fatalf("expected health error when ycql is stopped")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable, got %s", n.Code)
	}
}

func TestRuntimeYSQLControlAPIs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "tserver-ysql-control"
	cfg.EnableYSQL = true
	r := makeRuntimeForTest(t, cfg)

	stub := &ysqlStub{healthErr: dberrors.New(dberrors.ErrRetryableUnavailable, "ysql is not started", true, nil)}
	r.ysql = stub

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	status, err := r.GetYSQLStatus(ctx)
	if err != nil {
		t.Fatalf("get ysql status before start: %v", err)
	}
	if !status.Enabled {
		t.Fatalf("expected ysql enabled in status")
	}
	if status.Healthy {
		t.Fatalf("expected ysql unhealthy before start")
	}

	if err := r.StartYSQL(ctx); err != nil {
		t.Fatalf("start ysql: %v", err)
	}
	if stub.startCalls != 1 {
		t.Fatalf("expected one start call, got %d", stub.startCalls)
	}

	status, err = r.GetYSQLStatus(ctx)
	if err != nil {
		t.Fatalf("get ysql status after start: %v", err)
	}
	if !status.Enabled {
		t.Fatalf("expected ysql enabled in status after start")
	}
	if !status.Healthy {
		t.Fatalf("expected ysql healthy after start")
	}

	if err := r.StopYSQL(ctx); err != nil {
		t.Fatalf("stop ysql: %v", err)
	}
	if stub.stopCalls != 1 {
		t.Fatalf("expected one stop call, got %d", stub.stopCalls)
	}
}
