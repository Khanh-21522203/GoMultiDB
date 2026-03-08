package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"GoMultiDB/internal/query/cql"
	"GoMultiDB/internal/query/sql"
	rpcpkg "GoMultiDB/internal/rpc"
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

	mu      sync.Mutex
	started bool
	stopped atomic.Bool
}

type YSQLStatus struct {
	Enabled bool
	Healthy bool
}

func NewRuntime(cfg Config, rpcServer *rpcpkg.Server) (*Runtime, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node id is required")
	}
	if rpcServer == nil {
		return nil, fmt.Errorf("rpc server is required")
	}
	if cfg.MaxClockSkew <= 0 {
		cfg.MaxClockSkew = DefaultConfig().MaxClockSkew
	}
	return &Runtime{
		cfg:       cfg,
		rpcServer: rpcServer,
		clock:     systemClock{},
		sqlCoord:  sql.NewLocalCoordinator(),
		cqlServer: cql.NewLocalServer(),
	}, nil
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
		if err := r.sqlCoord.Start(ctx, sql.ProcessConfig{
			Enabled:        r.cfg.EnableYSQL,
			BindAddress:    r.cfg.YSQLBindAddress,
			MaxConnections: r.cfg.YSQLMaxConnections,
		}); err != nil {
			_ = r.rpcServer.Stop(context.Background())
			return err
		}
	}
	if r.cqlServer != nil {
		if err := r.cqlServer.Start(ctx, cql.Config{
			Enabled:        r.cfg.EnableYCQL,
			BindAddress:    r.cfg.YCQLBindAddress,
			MaxConnections: r.cfg.YCQLMaxConnections,
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

func (r *Runtime) Stop(ctx context.Context) error {
	if r.stopped.Swap(true) {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return nil
	}
	if r.cqlServer != nil {
		if err := r.cqlServer.Stop(ctx); err != nil {
			return err
		}
	}
	if r.sqlCoord != nil {
		if err := r.sqlCoord.Stop(ctx); err != nil {
			return err
		}
	}
	return r.rpcServer.Stop(ctx)
}

func (r *Runtime) StartYSQL(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if r.sqlCoord == nil {
		return nil
	}
	return r.sqlCoord.Start(ctx, sql.ProcessConfig{
		Enabled:        r.cfg.EnableYSQL,
		BindAddress:    r.cfg.YSQLBindAddress,
		MaxConnections: r.cfg.YSQLMaxConnections,
	})
}

func (r *Runtime) StopYSQL(ctx context.Context) error {
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

func (r *Runtime) GetYSQLStatus(ctx context.Context) (YSQLStatus, error) {
	select {
	case <-ctx.Done():
		return YSQLStatus{}, ctx.Err()
	default:
	}
	if r.sqlCoord == nil {
		return YSQLStatus{Enabled: false, Healthy: false}, nil
	}
	status := YSQLStatus{Enabled: r.cfg.EnableYSQL}
	if err := r.sqlCoord.Health(ctx); err != nil {
		return status, nil
	}
	status.Healthy = true
	return status, nil
}
