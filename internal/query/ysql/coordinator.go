package ysql

import (
	"context"
	"reflect"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
)

type ProcessConfig struct {
	Enabled        bool
	BindAddress    string
	MaxConnections int
	HBAConfig      string
	ExtraConf      map[string]string
}

type Coordinator interface {
	Start(ctx context.Context, cfg ProcessConfig) error
	Stop(ctx context.Context) error
	Health(ctx context.Context) error
}

type LocalCoordinator struct {
	mu      sync.RWMutex
	started bool
	cfg     ProcessConfig
}

func NewLocalCoordinator() *LocalCoordinator {
	return &LocalCoordinator{}
}

func (c *LocalCoordinator) Start(ctx context.Context, cfg ProcessConfig) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if cfg.Enabled {
		if cfg.BindAddress == "" {
			return dberrors.New(dberrors.ErrInvalidArgument, "ysql bind address is required when enabled", false, nil)
		}
		if cfg.MaxConnections <= 0 {
			cfg.MaxConnections = 300
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		if processConfigEqual(c.cfg, cfg) {
			return nil
		}
		return dberrors.New(dberrors.ErrConflict, "ysql coordinator already started with different config", false, nil)
	}
	c.started = true
	c.cfg = cfg
	return nil
}

func (c *LocalCoordinator) Stop(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = false
	c.cfg = ProcessConfig{}
	return nil
}

func (c *LocalCoordinator) Health(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.started {
		return dberrors.New(dberrors.ErrRetryableUnavailable, "ysql is not started", true, nil)
	}
	return nil
}

func processConfigEqual(a, b ProcessConfig) bool {
	if a.Enabled != b.Enabled || a.BindAddress != b.BindAddress || a.MaxConnections != b.MaxConnections || a.HBAConfig != b.HBAConfig {
		return false
	}
	return reflect.DeepEqual(a.ExtraConf, b.ExtraConf)
}
