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

type sqlStub struct {
	startCalls int
	stopCalls  int
	healthErr  error
}

func (s *sqlStub) Start(_ context.Context, _ sql.ProcessConfig) error {
	s.startCalls++
	s.healthErr = nil
	return nil
}

func (s *sqlStub) Stop(_ context.Context) error {
	s.stopCalls++
	s.healthErr = dberrors.New(dberrors.ErrRetryableUnavailable, "sql is not started", true, nil)
	return nil
}

func (s *sqlStub) Health(_ context.Context) error {
	return s.healthErr
}

type cqlStub struct {
	startCalls int
	stopCalls  int
	healthErr  error
}

func (s *cqlStub) Start(_ context.Context, _ cql.Config) error {
	s.startCalls++
	return nil
}

func (s *cqlStub) Stop(_ context.Context) error {
	s.stopCalls++
	return nil
}

func (s *cqlStub) Health(_ context.Context) error {
	return s.healthErr
}

func (s *cqlStub) Route(_ context.Context, _ cql.Request) (cql.Response, error) {
	if s.healthErr != nil {
		return cql.Response{}, s.healthErr
	}
	return cql.Response{Applied: true}, nil
}

func (s *cqlStub) RouteBatch(_ context.Context, _ any) (cql.Response, error) {
	if s.healthErr != nil {
		return cql.Response{}, s.healthErr
	}
	return cql.Response{Applied: true}, nil
}

func makeRuntimeForTest(t *testing.T, cfg Config) *Runtime {
	t.Helper()
	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{BindAddress: "127.0.0.1:0", StrictContractCheck: true})
	if err != nil {
		t.Fatalf("new rpc server: %v", err)
	}
	rocksStore := rocks.NewMemoryStore()
	r, err := NewRuntime(cfg, rpcServer, rocksStore)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return r
}

func TestRuntimeStartsAndStopsQueryCoordinators(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "tserver-test"
	r := makeRuntimeForTest(t, cfg)

	sqlS := &sqlStub{}
	cqlS := &cqlStub{}
	r.sqlCoord = sqlS
	r.cqlServer = cqlS

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	if sqlS.startCalls != 1 {
		t.Fatalf("expected one sql start call, got %d", sqlS.startCalls)
	}
	if cqlS.startCalls != 1 {
		t.Fatalf("expected one cql start call, got %d", cqlS.startCalls)
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("stop runtime: %v", err)
	}
	if sqlS.stopCalls != 1 {
		t.Fatalf("expected one sql stop call, got %d", sqlS.stopCalls)
	}
	if cqlS.stopCalls != 1 {
		t.Fatalf("expected one cql stop call, got %d", cqlS.stopCalls)
	}
}

func TestLocalSQLCoordinatorHealthRetryableWhenStopped(t *testing.T) {
	c := sql.NewLocalCoordinator()
	err := c.Health(context.Background())
	if err == nil {
		t.Fatalf("expected health error when sql is stopped")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable, got %s", n.Code)
	}
}

func TestLocalCQLServerHealthRetryableWhenStopped(t *testing.T) {
	s := cql.NewLocalServer()
	err := s.Health(context.Background())
	if err == nil {
		t.Fatalf("expected health error when cql is stopped")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable, got %s", n.Code)
	}
}

func TestRuntimeShutdownPhaseReachesZero(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "tserver-shutdown"
	r := makeRuntimeForTest(t, cfg)
	r.sqlCoord = &sqlStub{}
	r.cqlServer = &cqlStub{}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if phase := r.ShutdownPhase(); phase != 0 {
		t.Fatalf("expected shutdown phase 0 after clean stop, got %d", phase)
	}
}

func TestRuntimeYSQLControlAPIs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NodeID = "tserver-ysql-control"
	cfg.EnableYSQL = true
	r := makeRuntimeForTest(t, cfg)

	stub := &sqlStub{healthErr: dberrors.New(dberrors.ErrRetryableUnavailable, "sql is not started", true, nil)}
	r.sqlCoord = stub

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
