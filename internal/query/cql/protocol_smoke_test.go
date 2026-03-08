package cql

import (
	"context"
	"testing"
)

func TestPhase5SmokeCQLProtocolSurface(t *testing.T) {
	s := NewLocalServer()
	ctx := context.Background()

	if err := s.Start(ctx, Config{Enabled: true, BindAddress: "127.0.0.1:0", MaxConnections: 4}); err != nil {
		t.Fatalf("start cql server: %v", err)
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

	resp, err := s.Route(ctx, Request{ConnID: "conn-smoke", PreparedID: stmt.ID, Vars: []any{1}})
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

	resp, err = s.RouteBatch(ctx, map[string]any{
		"type": "LOGGED",
		"statements": []Request{
			{ConnID: "conn-smoke", Query: "UPDATE t SET v=? WHERE k=?", Vars: []any{"x", 1}},
			{ConnID: "conn-smoke", PreparedID: stmt.ID, Vars: []any{1}},
		},
	})
	if err != nil {
		t.Fatalf("route batch request: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected batch applied")
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
