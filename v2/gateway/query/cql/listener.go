package cql

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	errors "GoMultiDB/v2/contracts/errors"
)

// Listener accepts incoming CQL client connections on cfg.BindAddress and
// dispatches their frames to a LocalServer.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Listener struct {
	mu       sync.RWMutex
	cfg      Config
	server   *LocalServer
	listener net.Listener
	started  atomic.Bool
	stopped  atomic.Bool
	acceptWg sync.WaitGroup
}

// NewListener returns a Listener that will route accepted connections to
// server.
func NewListener(server *LocalServer) *Listener {
	return &Listener{}
}

// Start begins accepting CQL connections on cfg.BindAddress. A no-op if
// cfg.Enabled is false.
//
// Not yet implemented in this scaffold.
func (l *Listener) Start(ctx context.Context, cfg Config) error {
	return errors.ErrNotImplemented
}

// Stop closes the listener and waits for all connections to drain.
//
// Not yet implemented in this scaffold.
func (l *Listener) Stop(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// GetStatus returns the current listener status via its LocalServer.
//
// Not yet implemented in this scaffold.
func (l *Listener) GetStatus() (Status, error) {
	return Status{}, errors.ErrNotImplemented
}
