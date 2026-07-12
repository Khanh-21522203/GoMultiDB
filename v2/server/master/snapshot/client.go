package snapshot

import (
	"context"
	"net/http"

	errors "GoMultiDB/v2/contracts/errors"
)

// Client is an RPC client for tablet snapshot operations, calling a single
// tablet server over HTTP JSON-RPC.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Client struct {
	httpClient *http.Client
	baseURL    string
}

var _ TabletSnapshotRPC = (*Client)(nil)

// NewClient creates a new tablet snapshot RPC client targeting baseURL.
func NewClient(baseURL string) *Client {
	return &Client{}
}

// CreateTabletSnapshot creates a snapshot on the tablet.
//
// Not yet implemented in this scaffold.
func (c *Client) CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return errors.ErrNotImplemented
}

// DeleteTabletSnapshot deletes a snapshot from the tablet.
//
// Not yet implemented in this scaffold.
func (c *Client) DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return errors.ErrNotImplemented
}

// RestoreTabletSnapshot restores a snapshot on the tablet.
//
// Not yet implemented in this scaffold.
func (c *Client) RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return errors.ErrNotImplemented
}

// TabletRPCRegistry maps tablet IDs to their RPC endpoints. In production
// this is populated from the master's catalog and heartbeat state (see
// v2/server/master/registry).
type TabletRPCRegistry interface {
	// GetEndpoint returns the RPC endpoint for tabletID.
	GetEndpoint(tabletID string) (string, error)
}

// RegistryClient is a TabletSnapshotRPC implementation that resolves each
// call's target endpoint through a TabletRPCRegistry before dispatching.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type RegistryClient struct {
	registry TabletRPCRegistry
}

var _ TabletSnapshotRPC = (*RegistryClient)(nil)

// NewRegistryClient creates a new RegistryClient resolving endpoints
// through registry.
func NewRegistryClient(registry TabletRPCRegistry) *RegistryClient {
	return &RegistryClient{}
}

// CreateTabletSnapshot implements TabletSnapshotRPC.
//
// Not yet implemented in this scaffold.
func (c *RegistryClient) CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return errors.ErrNotImplemented
}

// DeleteTabletSnapshot implements TabletSnapshotRPC.
//
// Not yet implemented in this scaffold.
func (c *RegistryClient) DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return errors.ErrNotImplemented
}

// RestoreTabletSnapshot implements TabletSnapshotRPC.
//
// Not yet implemented in this scaffold.
func (c *RegistryClient) RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	return errors.ErrNotImplemented
}
