package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"GoMultiDB/internal/common/types"
	rpcpkg "GoMultiDB/internal/rpc"
)

// CDCProducerRPC is the RPC service definition for CDC streaming.
const (
	CDCServiceName        = "cdc.CDCProducer"
	CDCMethodGetChanges   = "GetChanges"
	CDCMethodSetCheckpoint = "SetCheckpoint"
)

// RPCGetChangesRequest is the RPC request payload for GetChanges (with TabletID).
type RPCGetChangesRequest struct {
	StreamID   string `json:"stream_id"`
	TabletID   string `json:"tablet_id"`
	AfterSeq   uint64 `json:"after_seq"`
	MaxRecords int    `json:"max_records"`
}

// RPCGetChangesResponse is the RPC response payload for GetChanges.
type RPCGetChangesResponse struct {
	TabletID   string  `json:"tablet_id"`
	Events     []Event `json:"events"`
	LatestSeen uint64  `json:"latest_seen"`
	Err        *string `json:"error,omitempty"`
}

// SetCheckpointRequest is the RPC request payload for SetCheckpoint.
type SetCheckpointRequest struct {
	StreamID   string     `json:"stream_id"`
	TabletID   string     `json:"tablet_id"`
	Checkpoint Checkpoint `json:"checkpoint"`
}

// SetCheckpointResponse is the RPC response for SetCheckpoint.
type SetCheckpointResponse struct {
	Err *string `json:"error,omitempty"`
}

// RPCProducer reads CDC events from a remote cluster via RPC.
type RPCProducer struct {
	client *rpcpkg.Client
}

// NewRPCProducer creates a producer that calls CDC RPCs on a remote node.
func NewRPCProducer(client *rpcpkg.Client) (*RPCProducer, error) {
	if client == nil {
		return nil, fmt.Errorf("rpc client is required")
	}
	return &RPCProducer{client: client}, nil
}

// GetChanges fetches CDC events from the source cluster.
func (p *RPCProducer) GetChanges(ctx context.Context, req RPCGetChangesRequest) (RPCGetChangesResponse, error) {
	payload, err := p.client.Call(ctx, types.RequestEnvelope{
		ContractVer: 1,
	}, CDCServiceName, CDCMethodGetChanges, mustMarshal(req))
	if err != nil {
		return RPCGetChangesResponse{}, fmt.Errorf("rpc GetChanges: %w", err)
	}
	var resp RPCGetChangesResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return RPCGetChangesResponse{}, fmt.Errorf("unmarshal response: %w", err)
	}
	if resp.Err != nil {
		return resp, fmt.Errorf("remote error: %s", *resp.Err)
	}
	return resp, nil
}

// SetCheckpoint persists a checkpoint on the source cluster.
func (p *RPCProducer) SetCheckpoint(ctx context.Context, req SetCheckpointRequest) error {
	payload, err := p.client.Call(ctx, types.RequestEnvelope{
		ContractVer: 1,
	}, CDCServiceName, CDCMethodSetCheckpoint, mustMarshal(req))
	if err != nil {
		return fmt.Errorf("rpc SetCheckpoint: %w", err)
	}
	var resp SetCheckpointResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if resp.Err != nil {
		return fmt.Errorf("remote error: %s", *resp.Err)
	}
	return nil
}

// Poller continuously pulls changes from a source RPCProducer and applies locally.
type Poller struct {
	producer    *RPCProducer
	streamID    string
	tabletID    string
	applier     Applier
	checkpointer CheckpointStore
	pollInterval time.Duration
	maxRecords   int
}

// Applier applies CDC events to the local cluster.
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
	Producer     *RPCProducer
	StreamID     string
	TabletID     string
	Applier      Applier
	Checkpointer CheckpointStore
	// PollInterval is the base backoff when no records are available. Default 100ms.
	PollInterval time.Duration
	// MaxRecords per poll. Default 1000.
	MaxRecords int
}

// NewPoller creates a Poller with the given config.
func NewPoller(cfg PollerConfig) (*Poller, error) {
	if cfg.Producer == nil {
		return nil, fmt.Errorf("producer is required")
	}
	if cfg.Applier == nil {
		return nil, fmt.Errorf("applier is required")
	}
	if cfg.Checkpointer == nil {
		return nil, fmt.Errorf("checkpointer is required")
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.MaxRecords == 0 {
		cfg.MaxRecords = 1000
	}
	return &Poller{
		producer:     cfg.Producer,
		streamID:     cfg.StreamID,
		tabletID:     cfg.TabletID,
		applier:      cfg.Applier,
		checkpointer: cfg.Checkpointer,
		pollInterval: cfg.PollInterval,
		maxRecords:   cfg.MaxRecords,
	}, nil
}

// RunOnce performs a single poll-apply-checkpoint cycle. Returns the number of events applied.
func (p *Poller) RunOnce(ctx context.Context) (int, error) {
	// Load current checkpoint.
	cp, err := p.checkpointer.GetCheckpoint(ctx, p.streamID, p.tabletID)
	if err != nil {
		return 0, fmt.Errorf("get checkpoint: %w", err)
	}

	// Fetch changes from source.
	resp, err := p.producer.GetChanges(ctx, RPCGetChangesRequest{
		StreamID:   p.streamID,
		TabletID:   p.tabletID,
		AfterSeq:   cp.Sequence,
		MaxRecords: p.maxRecords,
	})
	if err != nil {
		return 0, fmt.Errorf("get changes: %w", err)
	}

	// Apply events.
	for _, ev := range resp.Events {
		if err := p.applier.Apply(ctx, ev); err != nil {
			return 0, fmt.Errorf("apply event seq=%d: %w", ev.Sequence, err)
		}
	}

	// Advance checkpoint if we made progress.
	if len(resp.Events) > 0 {
		lastEv := resp.Events[len(resp.Events)-1]
		cp.Sequence = lastEv.Sequence
		cp.Timestamp = time.Now().UTC()
		if err := p.checkpointer.AdvanceCheckpoint(ctx, cp); err != nil {
			return len(resp.Events), fmt.Errorf("advance checkpoint: %w", err)
		}
	}

	return len(resp.Events), nil
}

// Run loops until context cancelled, backing off when idle.
func (p *Poller) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := p.RunOnce(ctx)
			if err != nil {
				return err
			}
			// If we got events, reset ticker to poll immediately.
			if n > 0 {
				ticker.Reset(p.pollInterval)
			}
		}
	}
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal json: %v", err))
	}
	return b
}

