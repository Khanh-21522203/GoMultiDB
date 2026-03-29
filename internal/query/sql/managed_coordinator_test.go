package sql

import (
	"context"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestManagedCoordinatorFallsBackWhenProcessUnavailable(t *testing.T) {
	c := NewManagedCoordinator()
	ctx := context.Background()

	err := c.Start(ctx, ProcessConfig{
		Enabled:                  true,
		BindAddress:              "127.0.0.1:5433",
		MaxConnections:           64,
		PreferProcess:            true,
		ProcessDataDir:           t.TempDir(),
		ProcessBinPath:           "/missing/postgres",
		ProcessInitDBPath:        "/missing/initdb",
		AllowCoordinatorFallback: true,
	})
	if err != nil {
		t.Fatalf("expected fallback start to succeed, got %v", err)
	}
	if c.UsingProcess() {
		t.Fatalf("expected local fallback coordinator mode")
	}
	if err := c.Health(ctx); err != nil {
		t.Fatalf("expected healthy fallback coordinator: %v", err)
	}
}

func TestManagedCoordinatorProcessRequiredReturnsError(t *testing.T) {
	c := NewManagedCoordinator()
	ctx := context.Background()

	err := c.Start(ctx, ProcessConfig{
		Enabled:                  true,
		BindAddress:              "127.0.0.1:5433",
		MaxConnections:           64,
		PreferProcess:            true,
		ProcessDataDir:           t.TempDir(),
		ProcessBinPath:           "/missing/postgres",
		ProcessInitDBPath:        "/missing/initdb",
		AllowCoordinatorFallback: false,
	})
	if err == nil {
		t.Fatalf("expected process start error when fallback disabled")
	}
}

func TestManagedCoordinatorStopTransitionsToUnhealthy(t *testing.T) {
	c := NewManagedCoordinator()
	ctx := context.Background()

	if err := c.Start(ctx, ProcessConfig{
		Enabled:                  true,
		BindAddress:              "127.0.0.1:5433",
		MaxConnections:           64,
		PreferProcess:            true,
		ProcessDataDir:           t.TempDir(),
		ProcessBinPath:           "/missing/postgres",
		ProcessInitDBPath:        "/missing/initdb",
		AllowCoordinatorFallback: true,
	}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	err := c.Health(ctx)
	if err == nil {
		t.Fatalf("expected unhealthy after stop")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected retryable unavailable, got %s", n.Code)
	}
}
