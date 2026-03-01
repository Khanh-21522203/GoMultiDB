package ysql

import (
	"context"
	"testing"
)

func TestPhase5SmokeYSQLCoordinatorSurface(t *testing.T) {
	ctx := context.Background()
	c := NewLocalCoordinator()

	if err := c.Health(ctx); err == nil {
		t.Fatalf("expected health failure before start")
	}

	if err := c.Start(ctx, ProcessConfig{Enabled: true, BindAddress: "127.0.0.1:5433", MaxConnections: 100}); err != nil {
		t.Fatalf("start ysql coordinator: %v", err)
	}
	if err := c.Health(ctx); err != nil {
		t.Fatalf("expected healthy coordinator after start: %v", err)
	}

	if err := c.Start(ctx, ProcessConfig{Enabled: true, BindAddress: "127.0.0.1:5433", MaxConnections: 100}); err != nil {
		t.Fatalf("expected idempotent start with same config: %v", err)
	}

	if err := c.Stop(ctx); err != nil {
		t.Fatalf("stop ysql coordinator: %v", err)
	}
	if err := c.Health(ctx); err == nil {
		t.Fatalf("expected health failure after stop")
	}
}
