package sql

import (
	"context"
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// ProcessConfig configures the SQL coordinator, including the optional
// postgres subprocess mode.
type ProcessConfig struct {
	Enabled                  bool
	BindAddress              string
	MaxConnections           int
	HBAConfig                string
	ExtraConf                map[string]string
	PreferProcess            bool
	ProcessDataDir           string
	ProcessBinPath           string
	ProcessInitDBPath        string
	ProcessStartTimeout      time.Duration
	ProcessStopTimeout       time.Duration
	AllowCoordinatorFallback bool
}

// Coordinator is the lifecycle surface for the SQL gateway.
type Coordinator interface {
	// Start begins serving SQL connections under cfg.
	Start(ctx context.Context, cfg ProcessConfig) error
	// Stop halts the coordinator, releasing all resources.
	Stop(ctx context.Context) error
	// Health reports whether the coordinator is currently able to serve
	// requests.
	Health(ctx context.Context) error
}

// LocalCoordinator is the in-process reference implementation of
// Coordinator: it tracks started/stopped state without delegating to a
// real postgres process.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type LocalCoordinator struct {
	mu      sync.RWMutex
	started bool
	cfg     ProcessConfig
}

var _ Coordinator = (*LocalCoordinator)(nil)

// NewLocalCoordinator returns an unstarted LocalCoordinator.
func NewLocalCoordinator() *LocalCoordinator {
	return &LocalCoordinator{}
}

// Start begins serving SQL connections under cfg.
//
// Not yet implemented in this scaffold.
func (c *LocalCoordinator) Start(ctx context.Context, cfg ProcessConfig) error {
	return errors.ErrNotImplemented
}

// Stop halts the coordinator, releasing all resources.
//
// Not yet implemented in this scaffold.
func (c *LocalCoordinator) Stop(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// Health reports whether the coordinator is currently able to serve
// requests.
//
// Not yet implemented in this scaffold.
func (c *LocalCoordinator) Health(ctx context.Context) error {
	return errors.ErrNotImplemented
}
