package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"GoMultiDB/internal/master/snapshot"
	"GoMultiDB/internal/query/cql"
	"GoMultiDB/internal/query/sql"
	rpcpkg "GoMultiDB/internal/rpc"
	"GoMultiDB/internal/storage/rocks"
)

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type Runtime struct {
	cfg Config

	rpcServer *rpcpkg.Server
	clock     Clock
	sqlCoord  sql.Coordinator
	cqlServer cql.Server

	// Snapshot coordinator for distributed snapshot management.
	snapCoord  *snapshot.Coordinator
	snapStore  snapshot.SnapshotStore
	rocksStore rocks.Store
	tabletRPC  snapshot.TabletRPCRegistry

	mu            sync.Mutex
	started       bool
	stopped       atomic.Bool
	shutdownPhase atomic.Int32
}

type SQLStatus struct {
	Enabled bool
	Healthy bool
}

func NewRuntime(cfg Config, rpcServer *rpcpkg.Server, rocksStore rocks.Store) (*Runtime, error) {
	return NewRuntimeWithTabletRPC(cfg, rpcServer, rocksStore, nil)
}

// NewRuntimeWithTabletRPC creates a Runtime with an optional tablet RPC registry.
// If tabletRPC is nil, the snapshot coordinator will use a no-op implementation.
func NewRuntimeWithTabletRPC(cfg Config, rpcServer *rpcpkg.Server, rocksStore rocks.Store, tabletRPC snapshot.TabletRPCRegistry) (*Runtime, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node id is required")
	}
	if rpcServer == nil {
		return nil, fmt.Errorf("rpc server is required")
	}
	if cfg.MaxClockSkew <= 0 {
		cfg.MaxClockSkew = DefaultConfig().MaxClockSkew
	}

	r := &Runtime{
		cfg:        cfg,
		rpcServer:  rpcServer,
		clock:      systemClock{},
		sqlCoord:   sql.NewManagedCoordinator(),
		cqlServer:  cql.NewLocalServer(),
		rocksStore: rocksStore,
		tabletRPC:  tabletRPC,
	}

	// Initialize snapshot coordinator if enabled.
	if cfg.EnableSnapshotCoord && rocksStore != nil {
		r.snapStore = snapshot.NewRocksSnapshotStore(rocksStore)

		// Use the provided registry or fall back to noop
		var rpcClient snapshot.TabletSnapshotRPC
		if tabletRPC != nil {
			rpcClient = snapshot.NewRegistryClient(tabletRPC)
		} else {
			rpcClient = &noopTabletSnapshotRPC{}
		}

		r.snapCoord = snapshot.NewCoordinator(rpcClient, snapshot.Config{
			MaxConcurrentSnapshots: cfg.MaxConcurrentSnaps,
			NowFn:                  r.clock.Now,
			Store:                  r.snapStore,
		})
	}

	return r, nil
}

func (r *Runtime) Init(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return nil
	}
	if err := r.rpcServer.Start(ctx); err != nil {
		return err
	}
	if r.sqlCoord != nil {
		if err := r.sqlCoord.Start(ctx, r.sqlProcessConfig()); err != nil {
			_ = r.rpcServer.Stop(context.Background())
			return err
		}
	}
	if r.cqlServer != nil {
		if err := r.cqlServer.Start(ctx, cql.Config{
			Enabled:        r.cfg.EnableCQL,
			BindAddress:    r.cfg.CQLBindAddress,
			MaxConnections: r.cfg.CQLMaxConnections,
		}); err != nil {
			if r.sqlCoord != nil {
				_ = r.sqlCoord.Stop(context.Background())
			}
			_ = r.rpcServer.Stop(context.Background())
			return err
		}
	}
	r.started = true
	return nil
}

// ShutdownPhase returns the current graceful-shutdown phase (0 = not stopping).
// Phases: 1=stop-query-coordinators, 2=stop-rpc.
func (r *Runtime) ShutdownPhase() int32 { return r.shutdownPhase.Load() }

func (r *Runtime) Stop(ctx context.Context) error {
	if r.stopped.Swap(true) {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return nil
	}

	// Phase 1 — stop query coordinators (CQL then SQL).
	r.shutdownPhase.Store(1)
	slog.Info("shutdown: phase 1 — stopping query coordinators")
	if r.cqlServer != nil {
		if err := r.cqlServer.Stop(ctx); err != nil {
			return fmt.Errorf("shutdown phase 1 (cql): %w", err)
		}
	}
	if r.sqlCoord != nil {
		if err := r.sqlCoord.Stop(ctx); err != nil {
			return fmt.Errorf("shutdown phase 1 (sql): %w", err)
		}
	}

	// Phase 2 — close RPC listener.
	r.shutdownPhase.Store(2)
	slog.Info("shutdown: phase 2 — stopping RPC server")
	if err := r.rpcServer.Stop(ctx); err != nil {
		return fmt.Errorf("shutdown phase 2 (rpc): %w", err)
	}

	r.shutdownPhase.Store(0)
	return nil
}

func (r *Runtime) StartSQL(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if r.sqlCoord == nil {
		return nil
	}
	return r.sqlCoord.Start(ctx, r.sqlProcessConfig())
}

func (r *Runtime) StopSQL(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if r.sqlCoord == nil {
		return nil
	}
	return r.sqlCoord.Stop(ctx)
}

func (r *Runtime) GetSQLStatus(ctx context.Context) (SQLStatus, error) {
	select {
	case <-ctx.Done():
		return SQLStatus{}, ctx.Err()
	default:
	}
	if r.sqlCoord == nil {
		return SQLStatus{Enabled: false, Healthy: false}, nil
	}
	status := SQLStatus{Enabled: r.cfg.EnableSQL}
	if err := r.sqlCoord.Health(ctx); err != nil {
		return status, nil
	}
	status.Healthy = true
	return status, nil
}

func (r *Runtime) sqlProcessConfig() sql.ProcessConfig {
	return sql.ProcessConfig{
		Enabled:                  r.cfg.EnableSQL,
		BindAddress:              r.cfg.SQLBindAddress,
		MaxConnections:           r.cfg.SQLMaxConnections,
		PreferProcess:            r.cfg.EnableSQLProcess,
		ProcessDataDir:           r.cfg.SQLDataDir,
		ProcessBinPath:           r.cfg.SQLProcessBinPath,
		ProcessInitDBPath:        r.cfg.SQLProcessInitDBPath,
		ProcessStartTimeout:      r.cfg.SQLProcessStartTimeout,
		ProcessStopTimeout:       r.cfg.SQLProcessStopTimeout,
		AllowCoordinatorFallback: r.cfg.SQLAllowFallbackToCoordinator,
	}
}

// GetSnapshotCoordinator returns the snapshot coordinator for this runtime.
// Returns nil if snapshot coordination is not enabled.
func (r *Runtime) GetSnapshotCoordinator() *snapshot.Coordinator {
	return r.snapCoord
}

// SetTabletRPCRegistry updates the tablet RPC registry used by the snapshot coordinator.
// This allows the registry to be populated after the runtime starts (e.g., after heartbeats register tablets).
func (r *Runtime) SetTabletRPCRegistry(registry snapshot.TabletRPCRegistry) {
	if r.snapCoord == nil {
		return
	}
	r.mu.Lock()
	r.tabletRPC = registry
	r.mu.Unlock()

	// Recreate the coordinator with the new registry
	rpcClient := snapshot.NewRegistryClient(registry)
	r.snapCoord = snapshot.NewCoordinator(rpcClient, snapshot.Config{
		MaxConcurrentSnapshots: r.cfg.MaxConcurrentSnaps,
		NowFn:                  r.clock.Now,
		Store:                  r.snapStore,
	})
}

// GetTabletRPCRegistry returns the current tablet RPC registry.
func (r *Runtime) GetTabletRPCRegistry() snapshot.TabletRPCRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tabletRPC
}

// noopTabletSnapshotRPC is a no-op implementation of TabletSnapshotRPC used
// until the real tablet snapshot gRPC client is implemented.
type noopTabletSnapshotRPC struct{}

func (noopTabletSnapshotRPC) CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	slog.Info("noop tablet snapshot: create", "snapshot_id", snapshotID, "tablet_id", tabletID)
	return nil
}

func (noopTabletSnapshotRPC) DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	slog.Info("noop tablet snapshot: delete", "snapshot_id", snapshotID, "tablet_id", tabletID)
	return nil
}

func (noopTabletSnapshotRPC) RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	slog.Info("noop tablet snapshot: restore", "snapshot_id", snapshotID, "tablet_id", tabletID)
	return nil
}
