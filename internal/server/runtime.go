package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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

	mu      sync.Mutex
	started bool
	stopped atomic.Bool
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
	return &Runtime{cfg: cfg, rpcServer: rpcServer, clock: systemClock{}}, nil
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
	return r.rpcServer.Stop(ctx)
}
