package sql

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
)

// ManagedCoordinator prefers a local postgres subprocess when configured and
// falls back to LocalCoordinator if process startup is unavailable.
type ManagedCoordinator struct {
	mu        sync.RWMutex
	started   bool
	cfg       ProcessConfig
	usingProc bool
	process   *PGProcess
	local     *LocalCoordinator
}

func NewManagedCoordinator() *ManagedCoordinator {
	return &ManagedCoordinator{
		local: NewLocalCoordinator(),
	}
}

func (c *ManagedCoordinator) Start(ctx context.Context, cfg ProcessConfig) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if cfg.Enabled {
		if cfg.BindAddress == "" {
			return dberrors.New(dberrors.ErrInvalidArgument, "sql bind address is required when enabled", false, nil)
		}
		if cfg.MaxConnections <= 0 {
			cfg.MaxConnections = 300
		}
	}

	c.mu.Lock()
	if c.started {
		if processConfigEqual(c.cfg, cfg) {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		return dberrors.New(dberrors.ErrConflict, "sql coordinator already started with different config", false, nil)
	}
	c.mu.Unlock()

	if cfg.PreferProcess {
		processCfg, err := buildPGProcessConfig(cfg)
		if err != nil {
			return err
		}
		proc, err := NewPGProcess(processCfg)
		if err != nil {
			return err
		}
		if err := proc.Start(ctx); err == nil {
			c.mu.Lock()
			c.started = true
			c.cfg = cfg
			c.usingProc = true
			c.process = proc
			c.mu.Unlock()
			return nil
		} else if !cfg.AllowCoordinatorFallback {
			return err
		}
	}

	if err := c.local.Start(ctx, cfg); err != nil {
		return err
	}
	c.mu.Lock()
	c.started = true
	c.cfg = cfg
	c.usingProc = false
	c.process = nil
	c.mu.Unlock()
	return nil
}

func (c *ManagedCoordinator) Stop(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}
	usingProc := c.usingProc
	proc := c.process
	c.started = false
	c.cfg = ProcessConfig{}
	c.usingProc = false
	c.process = nil
	c.mu.Unlock()

	if usingProc && proc != nil {
		return proc.Stop(ctx)
	}
	return c.local.Stop(ctx)
}

func (c *ManagedCoordinator) Health(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.RLock()
	started := c.started
	usingProc := c.usingProc
	proc := c.process
	c.mu.RUnlock()
	if !started {
		return dberrors.New(dberrors.ErrRetryableUnavailable, "sql is not started", true, nil)
	}
	if usingProc {
		if proc == nil {
			return dberrors.New(dberrors.ErrRetryableUnavailable, "sql process is not available", true, nil)
		}
		return proc.Health(ctx)
	}
	return c.local.Health(ctx)
}

func (c *ManagedCoordinator) UsingProcess() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.started && c.usingProc
}

func buildPGProcessConfig(cfg ProcessConfig) (PGProcessConfig, error) {
	host, portStr, err := net.SplitHostPort(cfg.BindAddress)
	if err != nil {
		return PGProcessConfig{}, dberrors.New(dberrors.ErrInvalidArgument, "sql bind address must be host:port for process mode", false, err)
	}
	port := 5433
	if portStr != "" {
		parsed, parseErr := parsePort(portStr)
		if parseErr != nil {
			return PGProcessConfig{}, dberrors.New(dberrors.ErrInvalidArgument, "invalid sql bind port for process mode", false, parseErr)
		}
		port = parsed
	}

	dataDir := strings.TrimSpace(cfg.ProcessDataDir)
	if dataDir == "" {
		dataDir = filepath.Join("/tmp", "gomultidb-sql")
	}

	return PGProcessConfig{
		DataDir:      dataDir,
		BindAddress:  host,
		Port:         port,
		BinPath:      cfg.ProcessBinPath,
		InitDBPath:   cfg.ProcessInitDBPath,
		HBAConfig:    cfg.HBAConfig,
		ExtraConf:    cfg.ExtraConf,
		StartTimeout: cfg.ProcessStartTimeout,
		StopTimeout:  cfg.ProcessStopTimeout,
	}, nil
}

func parsePort(port string) (int, error) {
	p, err := strconv.Atoi(port)
	if err != nil {
		return 0, err
	}
	if p <= 0 || p > 65535 {
		return 0, fmt.Errorf("port out of range: %d", p)
	}
	return p, nil
}
