package ycql

import (
	"context"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestLocalServerStartStopHealth(t *testing.T) {
	s := NewLocalServer()

	if err := s.Health(context.Background()); err == nil {
		t.Fatalf("expected health failure before start")
	}

	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("health after start: %v", err)
	}

	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := s.Health(context.Background()); err == nil {
		t.Fatalf("expected health failure after stop")
	}
}

func TestLocalServerStartIdempotentSameConfig(t *testing.T) {
	s := NewLocalServer()
	cfg := Config{Enabled: true, BindAddress: "127.0.0.1:9042", MaxConnections: 128}
	if err := s.Start(context.Background(), cfg); err != nil {
		t.Fatalf("start first: %v", err)
	}
	if err := s.Start(context.Background(), cfg); err != nil {
		t.Fatalf("start second idempotent: %v", err)
	}
}

func TestLocalServerStartConflictDifferentConfig(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042"}); err != nil {
		t.Fatalf("start first: %v", err)
	}
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9142"}); err == nil {
		t.Fatalf("expected config conflict on second start")
	}
}

func TestLocalServerValidation(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true}); err == nil {
		t.Fatalf("expected validation error for missing bind address")
	}
}

func TestLocalServerRoute(t *testing.T) {
	s := NewLocalServer()
	if _, err := s.Route(context.Background(), Request{Query: "SELECT now()"}); err == nil {
		t.Fatalf("expected route to fail when not started")
	}

	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	resp, err := s.Route(context.Background(), Request{Query: "SELECT now()"})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected applied response")
	}
}

func TestLocalServerRouteBatch(t *testing.T) {
	s := NewLocalServer()
	if _, err := s.RouteBatch(context.Background(), BatchRequest{Statements: []Request{{Query: "INSERT INTO t(k) VALUES (?)"}}}); err == nil {
		t.Fatalf("expected batch route to fail when not started")
	}

	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	resp, err := s.RouteBatch(context.Background(), BatchRequest{
		Type: BatchTypeLogged,
		Statements: []Request{
			{Query: "INSERT INTO t(k) VALUES (?)", Vars: []Value{1}},
			{Query: "UPDATE t SET v=? WHERE k=?", Vars: []Value{"v", 1}},
		},
	})
	if err != nil {
		t.Fatalf("route batch: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected batch applied response")
	}
	if resp.Rows != 2 {
		t.Fatalf("expected rows=2 for batch skeleton, got %d", resp.Rows)
	}
}

func TestLocalServerRouteBatchValidation(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, err := s.RouteBatch(context.Background(), BatchRequest{}); err == nil {
		t.Fatalf("expected validation failure for empty batch")
	}
	if _, err := s.RouteBatch(context.Background(), BatchRequest{Type: BatchType("BAD"), Statements: []Request{{Query: "SELECT 1"}}}); err == nil {
		t.Fatalf("expected validation failure for invalid batch type")
	}
	if _, err := s.RouteBatch(context.Background(), BatchRequest{Type: BatchTypeUnlogged, Statements: []Request{{Query: ""}}}); err == nil {
		t.Fatalf("expected validation failure for empty statement query")
	}
}

func TestLocalServerPreparedDispatch(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042", MaxConnections: 2}); err != nil {
		t.Fatalf("start: %v", err)
	}

	stmt, err := s.Prepare(context.Background(), "conn-1", "SELECT k FROM t WHERE k=?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	resp, err := s.Route(context.Background(), Request{ConnID: "conn-1", PreparedID: stmt.ID, Vars: []Value{1}})
	if err != nil {
		t.Fatalf("prepared route: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected applied prepared response")
	}
}

func TestLocalServerPreparedDispatchValidation(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	_, err := s.Route(context.Background(), Request{PreparedID: "stmt-1", Vars: []Value{1}})
	if err == nil {
		t.Fatalf("expected connection validation for prepared execution")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}
}

func TestLocalServerPreparedBatchDispatch(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042", MaxConnections: 3}); err != nil {
		t.Fatalf("start: %v", err)
	}

	stmt, err := s.Prepare(context.Background(), "conn-2", "SELECT v FROM t WHERE k=?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	resp, err := s.RouteBatch(context.Background(), BatchRequest{
		Type: BatchTypeLogged,
		Statements: []Request{
			{ConnID: "conn-2", PreparedID: stmt.ID, Vars: []Value{7}},
			{ConnID: "conn-2", Query: "UPDATE t SET v=? WHERE k=?", Vars: []Value{"x", 7}},
		},
	})
	if err != nil {
		t.Fatalf("route batch prepared: %v", err)
	}
	if !resp.Applied || resp.Rows != 2 {
		t.Fatalf("expected applied batch rows=2")
	}
}

func TestLocalServerConnectionLimitGuardrail(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042", MaxConnections: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := s.OpenConnection(context.Background(), "conn-a"); err != nil {
		t.Fatalf("open first connection: %v", err)
	}
	if err := s.OpenConnection(context.Background(), "conn-b"); err == nil {
		t.Fatalf("expected connection limit error")
	}

	if err := s.CloseConnection(context.Background(), "conn-a"); err != nil {
		t.Fatalf("close first connection: %v", err)
	}
	if err := s.OpenConnection(context.Background(), "conn-b"); err != nil {
		t.Fatalf("open second connection after close: %v", err)
	}
}

func TestLocalServerStatusSnapshot(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:9042", MaxConnections: 2}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := s.OpenConnection(context.Background(), "conn-1"); err != nil {
		t.Fatalf("open conn-1: %v", err)
	}
	if err := s.OpenConnection(context.Background(), "conn-2"); err != nil {
		t.Fatalf("open conn-2: %v", err)
	}
	stmt, err := s.Prepare(context.Background(), "conn-1", "SELECT 1")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := s.Route(context.Background(), Request{ConnID: "conn-1", PreparedID: stmt.ID}); err != nil {
		t.Fatalf("execute prepared hit: %v", err)
	}
	if _, err := s.Route(context.Background(), Request{ConnID: "conn-1", PreparedID: "missing"}); err == nil {
		t.Fatalf("expected prepared miss")
	}

	status, err := s.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !status.Started {
		t.Fatalf("expected started=true")
	}
	if status.MaxConnections != 2 {
		t.Fatalf("expected max connections=2, got %d", status.MaxConnections)
	}
	if status.ActiveConnections != 2 {
		t.Fatalf("expected active connections=2, got %d", status.ActiveConnections)
	}
	if status.Prepared.CacheHits < 1 {
		t.Fatalf("expected prepared cache hits >= 1, got %d", status.Prepared.CacheHits)
	}
	if status.Prepared.CacheMisses < 1 {
		t.Fatalf("expected prepared cache misses >= 1, got %d", status.Prepared.CacheMisses)
	}
}
