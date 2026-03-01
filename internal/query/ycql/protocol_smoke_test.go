package ycql

import (
	"context"
	"testing"
)

func TestPhase5SmokeYCQLProtocolSurface(t *testing.T) {
	s := NewLocalServer()
	ctx := context.Background()

	if err := s.Start(ctx, Config{Enabled: true, BindAddress: "127.0.0.1:9042", MaxConnections: 4}); err != nil {
		t.Fatalf("start ycql server: %v", err)
	}
	defer func() {
		_ = s.Stop(ctx)
	}()

	if err := s.OpenConnection(ctx, "conn-smoke"); err != nil {
		t.Fatalf("open connection: %v", err)
	}

	stmt, err := s.Prepare(ctx, "conn-smoke", "SELECT v FROM t WHERE k=?")
	if err != nil {
		t.Fatalf("prepare statement: %v", err)
	}

	resp, err := s.Route(ctx, Request{ConnID: "conn-smoke", PreparedID: stmt.ID, Vars: []Value{1}})
	if err != nil {
		t.Fatalf("route prepared request: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected prepared route applied")
	}

	resp, err = s.Route(ctx, Request{ConnID: "conn-smoke", Query: "SELECT now()"})
	if err != nil {
		t.Fatalf("route raw request: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected raw route applied")
	}

	resp, err = s.RouteBatch(ctx, BatchRequest{
		Type: BatchTypeLogged,
		Statements: []Request{
			{ConnID: "conn-smoke", Query: "UPDATE t SET v=? WHERE k=?", Vars: []Value{"x", 1}},
			{ConnID: "conn-smoke", PreparedID: stmt.ID, Vars: []Value{1}},
		},
	})
	if err != nil {
		t.Fatalf("route batch request: %v", err)
	}
	if !resp.Applied || resp.Rows != 2 {
		t.Fatalf("expected batch applied with rows=2, got applied=%v rows=%d", resp.Applied, resp.Rows)
	}

	status, err := s.Status(ctx)
	if err != nil {
		t.Fatalf("status snapshot: %v", err)
	}
	if !status.Started {
		t.Fatalf("expected started status=true")
	}
	if status.ActiveConnections != 1 {
		t.Fatalf("expected active connections=1, got %d", status.ActiveConnections)
	}
	if status.Prepared.CacheHits == 0 {
		t.Fatalf("expected prepared cache hits > 0")
	}
}
