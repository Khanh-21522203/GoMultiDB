package sql

import (
	"context"
	"sync"

	errors "GoMultiDB/v2/contracts/errors"
)

// ManagedCoordinator prefers a local postgres subprocess (PGProcess) when
// cfg.PreferProcess is set, falling back to a LocalCoordinator if process
// startup is unavailable and cfg.AllowCoordinatorFallback allows it.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type ManagedCoordinator struct {
	mu        sync.RWMutex
	started   bool
	cfg       ProcessConfig
	usingProc bool
	process   *PGProcess
	local     *LocalCoordinator
}

var _ Coordinator = (*ManagedCoordinator)(nil)

// NewManagedCoordinator returns an unstarted ManagedCoordinator.
func NewManagedCoordinator() *ManagedCoordinator {
	return &ManagedCoordinator{}
}

// Start begins serving SQL connections under cfg, preferring a postgres
// subprocess when cfg.PreferProcess is set.
//
// Not yet implemented in this scaffold.
func (c *ManagedCoordinator) Start(ctx context.Context, cfg ProcessConfig) error {
	return errors.ErrNotImplemented
}

// Stop halts the coordinator, stopping its subprocess or delegate as
// appropriate.
//
// Not yet implemented in this scaffold.
func (c *ManagedCoordinator) Stop(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// Health reports whether the coordinator (subprocess or delegate) is
// currently able to serve requests.
//
// Not yet implemented in this scaffold.
func (c *ManagedCoordinator) Health(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// UsingProcess reports whether the coordinator is currently backed by a
// postgres subprocess rather than the LocalCoordinator delegate.
//
// Not yet implemented in this scaffold.
func (c *ManagedCoordinator) UsingProcess() bool {
	panic("not implemented")
}
