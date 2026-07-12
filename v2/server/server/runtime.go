package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/gateway/query/cql"
	"GoMultiDB/v2/gateway/query/sql"
	rpcpkg "GoMultiDB/v2/infra/rpc"
	"GoMultiDB/v2/infra/storage/rocks"
	"GoMultiDB/v2/server/master/snapshot"
)

// Clock returns the current time. It is injectable for tests.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

// systemClock is the default Clock, backed by the wall clock.
type systemClock struct{}

var _ Clock = systemClock{}

// Now returns time.Now().UTC().
func (systemClock) Now() time.Time { return time.Now().UTC() }

// Runtime wires together a node's RPC server, query-gateway coordinators,
// and (when enabled) snapshot coordinator, and drives their combined
// start-up and phased graceful shutdown.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented or panic.
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

// SQLStatus reports the SQL gateway's operating state.
type SQLStatus struct {
	Enabled bool
	Healthy bool
}

// NewRuntime creates a Runtime with no tablet RPC registry configured.
// Equivalent to NewRuntimeWithTabletRPC(cfg, rpcServer, rocksStore, nil).
func NewRuntime(cfg Config, rpcServer *rpcpkg.Server, rocksStore rocks.Store) (*Runtime, error) {
	return &Runtime{}, nil
}

// NewRuntimeWithTabletRPC creates a Runtime with an optional tablet RPC
// registry. If tabletRPC is nil, the snapshot coordinator uses a no-op
// implementation. Returns an error if cfg.NodeID is empty or rpcServer is
// nil.
func NewRuntimeWithTabletRPC(cfg Config, rpcServer *rpcpkg.Server, rocksStore rocks.Store, tabletRPC snapshot.TabletRPCRegistry) (*Runtime, error) {
	return &Runtime{}, nil
}

// Init performs any pre-Start initialization for the runtime.
//
// Not yet implemented in this scaffold.
func (r *Runtime) Init(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// Start starts the RPC server and, if configured, the SQL and CQL
// coordinators.
//
// Not yet implemented in this scaffold.
func (r *Runtime) Start(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// ShutdownPhase returns the current graceful-shutdown phase (0 = not
// stopping). Phases: 1=stop-query-coordinators, 2=stop-rpc.
//
// Not yet implemented in this scaffold.
func (r *Runtime) ShutdownPhase() int32 {
	panic("not implemented")
}

// Stop gracefully shuts down the runtime: first the query coordinators
// (CQL then SQL), then the RPC server.
//
// Not yet implemented in this scaffold.
func (r *Runtime) Stop(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// StartSQL starts the SQL coordinator, if configured.
//
// Not yet implemented in this scaffold.
func (r *Runtime) StartSQL(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// StopSQL stops the SQL coordinator, if configured.
//
// Not yet implemented in this scaffold.
func (r *Runtime) StopSQL(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// GetSQLStatus reports the SQL coordinator's current operating state.
//
// Not yet implemented in this scaffold.
func (r *Runtime) GetSQLStatus(ctx context.Context) (SQLStatus, error) {
	return SQLStatus{}, errors.ErrNotImplemented
}

// GetSnapshotCoordinator returns the snapshot coordinator for this
// runtime. Returns nil if snapshot coordination is not enabled.
//
// Not yet implemented in this scaffold.
func (r *Runtime) GetSnapshotCoordinator() *snapshot.Coordinator {
	panic("not implemented")
}

// SetTabletRPCRegistry updates the tablet RPC registry used by the
// snapshot coordinator. This allows the registry to be populated after the
// runtime starts (e.g., after heartbeats register tablets).
//
// Not yet implemented in this scaffold.
func (r *Runtime) SetTabletRPCRegistry(registry snapshot.TabletRPCRegistry) {
	panic("not implemented")
}

// GetTabletRPCRegistry returns the current tablet RPC registry.
//
// Not yet implemented in this scaffold.
func (r *Runtime) GetTabletRPCRegistry() snapshot.TabletRPCRegistry {
	panic("not implemented")
}
