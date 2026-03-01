package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"GoMultiDB/internal/query/ycql"
	"GoMultiDB/internal/query/ysql"
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
	ysql      ysql.Coordinator
	ycql      ycql.Server

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
		ysql:      ysql.NewLocalCoordinator(),
		ycql:      ycql.NewLocalServer(),
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
	if r.ysql != nil {
		if err := r.ysql.Start(ctx, ysql.ProcessConfig{
			Enabled:        r.cfg.EnableYSQL,
			BindAddress:    r.cfg.YSQLBindAddress,
			MaxConnections: r.cfg.YSQLMaxConnections,
		}); err != nil {
			_ = r.rpcServer.Stop(context.Background())
			return err
		}
	}
	if r.ycql != nil {
		if err := r.ycql.Start(ctx, ycql.Config{
			Enabled:        r.cfg.EnableYCQL,
			BindAddress:    r.cfg.YCQLBindAddress,
			MaxConnections: r.cfg.YCQLMaxConnections,
		}); err != nil {
			if r.ysql != nil {
				_ = r.ysql.Stop(context.Background())
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
	if r.ycql != nil {
		if err := r.ycql.Stop(ctx); err != nil {
			return err
		}
	}
	if r.ysql != nil {
		if err := r.ysql.Stop(ctx); err != nil {
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
	if r.ysql == nil {
		return nil
	}
	return r.ysql.Start(ctx, ysql.ProcessConfig{
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
	if r.ysql == nil {
		return nil
	}
	return r.ysql.Stop(ctx)
}

func (r *Runtime) GetYSQLStatus(ctx context.Context) (YSQLStatus, error) {
	select {
	case <-ctx.Done():
		return YSQLStatus{}, ctx.Err()
	default:
	}
	if r.ysql == nil {
		return YSQLStatus{Enabled: false, Healthy: false}, nil
	}
	status := YSQLStatus{Enabled: r.cfg.EnableYSQL}
	if err := r.ysql.Health(ctx); err != nil {
		return status, nil
	}
	status.Healthy = true
	return status, nil
}
