package cql

import (
	"bytes"
	"context"
	"strings"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestLocalServerStartStopHealth(t *testing.T) {
	s := NewLocalServer()

	if err := s.Health(context.Background()); err == nil {
		t.Fatalf("expected health failure before start")
	}

	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
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
	cfg := Config{Enabled: true, BindAddress: "127.0.0.1:0", MaxConnections: 128}
	if err := s.Start(context.Background(), cfg); err != nil {
		t.Fatalf("start first: %v", err)
	}
	if err := s.Start(context.Background(), cfg); err != nil {
		t.Fatalf("start second idempotent: %v", err)
	}
}

func TestLocalServerStartConflictDifferentConfig(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
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

	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
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
	if _, err := s.RouteBatch(context.Background(), nil); err == nil {
		t.Fatalf("expected batch route to fail when not started")
	}

	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	resp, err := s.RouteBatch(context.Background(), map[string]any{
		"statements": []Request{{ConnID: "batch-conn", Query: "INSERT INTO t (k) VALUES (1)"}},
	})
	if err != nil {
		t.Fatalf("route batch: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected batch applied response")
	}

	if _, err := s.RouteBatch(context.Background(), map[string]any{}); err == nil {
		t.Fatalf("expected validation error for empty batch request")
	}
}

func TestLocalServerRouteBatchValidation(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Validation tests simplified since RouteBatch now accepts any type.
	resp, err := s.RouteBatch(context.Background(), map[string]any{"statements": []Request{{Query: "SELECT 1"}}})
	if err != nil {
		t.Fatalf("route batch with statements: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected batch applied response")
	}
}

func TestLocalServerPreparedDispatch(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0", MaxConnections: 2}); err != nil {
		t.Fatalf("start: %v", err)
	}

	stmt, err := s.Prepare(context.Background(), "conn-1", "SELECT k FROM t WHERE k=?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	resp, err := s.Route(context.Background(), Request{ConnID: "conn-1", PreparedID: stmt.ID, Vars: []any{1}})
	if err != nil {
		t.Fatalf("prepared route: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected applied prepared response")
	}
}

func TestLocalServerPreparedDispatchValidation(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	_, err := s.Route(context.Background(), Request{PreparedID: "stmt-1", Vars: []any{1}})
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
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0", MaxConnections: 3}); err != nil {
		t.Fatalf("start: %v", err)
	}

	stmt, err := s.Prepare(context.Background(), "conn-2", "SELECT v FROM t WHERE k=?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	resp, err := s.RouteBatch(context.Background(), map[string]any{
		"type":    "LOGGED",
		"conn_id": "conn-2",
		"queries": []BatchQuery{
			{Kind: 1, QueryID: []byte(stmt.ID)},
			{Kind: 0, QueryString: "UPDATE t SET v=? WHERE k=?"},
		},
	})
	if err != nil {
		t.Fatalf("route batch prepared: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected applied batch")
	}

	selectResp, err := s.Route(context.Background(), Request{ConnID: "conn-2", Query: "SELECT * FROM t"})
	if err != nil {
		t.Fatalf("select after prepared batch: %v", err)
	}
	if selectResp.Rows < 1 {
		t.Fatalf("expected at least one row after batch writes, got %d", selectResp.Rows)
	}
}

func TestLocalServerConnectionLimitGuardrail(t *testing.T) {
	s := NewLocalServer()
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0", MaxConnections: 1}); err != nil {
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
	if err := s.Start(context.Background(), Config{Enabled: true, BindAddress: "127.0.0.1:0", MaxConnections: 2}); err != nil {
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

func TestLocalServerRoutesCoreDMLThroughExecutionEngine(t *testing.T) {
	s := NewLocalServer()
	ctx := context.Background()
	if err := s.Start(ctx, Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	connID := "conn-dml"
	if _, err := s.Route(ctx, Request{ConnID: connID, Query: "INSERT INTO users (k, v) VALUES (1, 'a')"}); err != nil {
		t.Fatalf("insert #1: %v", err)
	}
	if _, err := s.Route(ctx, Request{ConnID: connID, Query: "INSERT INTO users (k, v) VALUES (2, 'b')"}); err != nil {
		t.Fatalf("insert #2: %v", err)
	}

	read, err := s.Route(ctx, Request{ConnID: connID, Query: "SELECT * FROM users"})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if read.Rows != 2 {
		t.Fatalf("expected rows=2 after inserts, got %d", read.Rows)
	}

	if _, err := s.Route(ctx, Request{ConnID: connID, Query: "DELETE FROM users WHERE k=1"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	read, err = s.Route(ctx, Request{ConnID: connID, Query: "SELECT * FROM users"})
	if err != nil {
		t.Fatalf("select after delete: %v", err)
	}
	if read.Rows != 1 {
		t.Fatalf("expected rows=1 after delete, got %d", read.Rows)
	}
}

func TestListenerReturnsErrorForUnsupportedRegisterAndAuthOpcodes(t *testing.T) {
	s := NewLocalServer()
	ctx := context.Background()
	if err := s.Start(ctx, Config{Enabled: true, BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	codec := NewCodec()

	reg := &RegisterRequest{Events: []string{"TOPOLOGY_CHANGE"}}
	buf := &bytes.Buffer{}
	if err := reg.Marshal(codec, buf); err != nil {
		t.Fatalf("marshal register: %v", err)
	}
	resp := s.listener.handleFrame(ctx, codec, "listener-test", &Frame{
		Version: protocolVersion,
		Stream:  1,
		Opcode:  OpcodeRegister,
		Body:    buf.Bytes(),
	})
	if resp.Opcode != OpcodeError {
		t.Fatalf("expected register to return ERROR, got opcode=%d", resp.Opcode)
	}
	msg := decodeErrorMessage(t, codec, resp.Body)
	if !strings.Contains(strings.ToLower(msg), "not supported") {
		t.Fatalf("expected unsupported message, got %q", msg)
	}

	resp = s.listener.handleFrame(ctx, codec, "listener-test", &Frame{
		Version: protocolVersion,
		Stream:  2,
		Opcode:  OpcodeAuthResponse,
	})
	if resp.Opcode != OpcodeError {
		t.Fatalf("expected auth opcode to return ERROR, got opcode=%d", resp.Opcode)
	}
	msg = decodeErrorMessage(t, codec, resp.Body)
	if !strings.Contains(strings.ToLower(msg), "not supported") {
		t.Fatalf("expected auth unsupported message, got %q", msg)
	}
}

func decodeErrorMessage(t *testing.T, codec *Codec, body []byte) string {
	t.Helper()
	rd := bytes.NewReader(body)
	if _, err := codec.ReadInt(rd); err != nil {
		t.Fatalf("read error code: %v", err)
	}
	msg, err := codec.ReadString(rd)
	if err != nil {
		t.Fatalf("read error message: %v", err)
	}
	return msg
}
