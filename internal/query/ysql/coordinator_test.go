package ysql

import (
	"context"
	"testing"
)

func TestLocalCoordinatorStartStopHealth(t *testing.T) {
	c := NewLocalCoordinator()

	if err := c.Health(context.Background()); err == nil {
		t.Fatalf("expected health failure before start")
	}

	if err := c.Start(context.Background(), ProcessConfig{Enabled: true, BindAddress: "127.0.0.1:5433"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("health after start: %v", err)
	}

	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := c.Health(context.Background()); err == nil {
		t.Fatalf("expected health failure after stop")
	}
}

func TestLocalCoordinatorStartIdempotentSameConfig(t *testing.T) {
	c := NewLocalCoordinator()
	cfg := ProcessConfig{Enabled: true, BindAddress: "127.0.0.1:5433", MaxConnections: 128}
	if err := c.Start(context.Background(), cfg); err != nil {
		t.Fatalf("start first: %v", err)
	}
	if err := c.Start(context.Background(), cfg); err != nil {
		t.Fatalf("start second idempotent: %v", err)
	}
}

func TestLocalCoordinatorStartConflictDifferentConfig(t *testing.T) {
	c := NewLocalCoordinator()
	if err := c.Start(context.Background(), ProcessConfig{Enabled: true, BindAddress: "127.0.0.1:5433"}); err != nil {
		t.Fatalf("start first: %v", err)
	}
	if err := c.Start(context.Background(), ProcessConfig{Enabled: true, BindAddress: "127.0.0.1:6433"}); err == nil {
		t.Fatalf("expected config conflict on second start")
	}
}

func TestLocalCoordinatorValidation(t *testing.T) {
	c := NewLocalCoordinator()
	if err := c.Start(context.Background(), ProcessConfig{Enabled: true}); err == nil {
		t.Fatalf("expected validation error for missing bind address")
	}
}
