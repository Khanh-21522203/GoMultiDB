package cdc

import (
	"context"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
	rpcpkg "GoMultiDB/v2/infra/rpc"
)

// RPC service and method names for CDC streaming.
const (
	CDCServiceName         = "cdc.CDCProducer"
	CDCMethodGetChanges    = "GetChanges"
	CDCMethodSetCheckpoint = "SetCheckpoint"
)

// RPCGetChangesRequest is the RPC request payload for CDCMethodGetChanges.
type RPCGetChangesRequest struct {
	StreamID   string `json:"stream_id"`
	TabletID   string `json:"tablet_id"`
	AfterSeq   uint64 `json:"after_seq"`
	MaxRecords int    `json:"max_records"`
}

// RPCGetChangesResponse is the RPC response payload for
// CDCMethodGetChanges.
type RPCGetChangesResponse struct {
	TabletID   string  `json:"tablet_id"`
	Events     []Event `json:"events"`
	LatestSeen uint64  `json:"latest_seen"`
	Err        *string `json:"error,omitempty"`
}

// SetCheckpointRequest is the RPC request payload for
// CDCMethodSetCheckpoint.
type SetCheckpointRequest struct {
	StreamID   string     `json:"stream_id"`
	TabletID   string     `json:"tablet_id"`
	Checkpoint Checkpoint `json:"checkpoint"`
}

// SetCheckpointResponse is the RPC response payload for
// CDCMethodSetCheckpoint.
type SetCheckpointResponse struct {
	Err *string `json:"error,omitempty"`
}

// RPCProducer reads CDC events from a remote cluster over RPC.
//
// Scaffold stub: GetChanges and SetCheckpoint return
// errors.ErrNotImplemented.
type RPCProducer struct {
	client *rpcpkg.Client
}

// NewRPCProducer returns an RPCProducer that calls CDC RPCs through
// client.
func NewRPCProducer(client *rpcpkg.Client) (*RPCProducer, error) {
	return &RPCProducer{}, nil
}

// GetChanges fetches CDC events from the source cluster.
//
// Not yet implemented in this scaffold.
func (p *RPCProducer) GetChanges(ctx context.Context, req RPCGetChangesRequest) (RPCGetChangesResponse, error) {
	return RPCGetChangesResponse{}, dberrors.ErrNotImplemented
}

// SetCheckpoint persists a checkpoint on the source cluster.
//
// Not yet implemented in this scaffold.
func (p *RPCProducer) SetCheckpoint(ctx context.Context, req SetCheckpointRequest) error {
	return dberrors.ErrNotImplemented
}

// Applier applies a single CDC event to the local cluster.
type Applier interface {
	Apply(ctx context.Context, ev Event) error
}

// CheckpointStore persists CDC stream checkpoints.
type CheckpointStore interface {
	AdvanceCheckpoint(ctx context.Context, cp Checkpoint) error
	GetCheckpoint(ctx context.Context, streamID, tabletID string) (Checkpoint, error)
}

// PollerConfig configures a Poller.
type PollerConfig struct {
	Producer *RPCProducer
	StreamID string
	TabletID string
	Applier  Applier
	// Checkpointer persists progress for this poller's stream/tablet.
	Checkpointer CheckpointStore
	// PollInterval is the base backoff when no records are available.
	// Defaults to 100ms.
	PollInterval time.Duration
	// MaxRecords caps the number of events fetched per poll. Defaults to
	// 1000.
	MaxRecords int
}

// Poller continuously pulls changes from a source RPCProducer and applies
// them locally, advancing its checkpoint as it makes progress.
//
// Scaffold stub: RunOnce and Run return errors.ErrNotImplemented.
type Poller struct {
	producer     *RPCProducer
	streamID     string
	tabletID     string
	applier      Applier
	checkpointer CheckpointStore
	pollInterval time.Duration
	maxRecords   int
}

// NewPoller returns a Poller configured by cfg.
func NewPoller(cfg PollerConfig) (*Poller, error) {
	return &Poller{}, nil
}

// RunOnce performs a single poll-apply-checkpoint cycle and returns the
// number of events applied.
//
// Not yet implemented in this scaffold.
func (p *Poller) RunOnce(ctx context.Context) (int, error) {
	return 0, dberrors.ErrNotImplemented
}

// Run loops until ctx is cancelled, backing off when idle and polling
// immediately again after making progress.
//
// Not yet implemented in this scaffold.
func (p *Poller) Run(ctx context.Context) error {
	return dberrors.ErrNotImplemented
}
