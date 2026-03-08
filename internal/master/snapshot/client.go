// Package snapshot provides the RPC client for calling tablet snapshot operations.
package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	dberrors "GoMultiDB/internal/common/errors"
)

// Client is an RPC client for tablet snapshot operations.
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// NewClient creates a new tablet snapshot RPC client.
func NewClient(baseURL string) *Client {
	return &Client{
		httpClient: &http.Client{},
		baseURL:    baseURL,
	}
}

// CreateTabletSnapshot creates a snapshot on the tablet.
func (c *Client) CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	req := tabletSnapshotRequest{
		Service:    "tablet_snapshot",
		Method:     "create_tablet_snapshot",
		Payload:    createTabletSnapshotRequest{SnapshotID: snapshotID, TabletID: tabletID, CreateHT: 0},
		RequestID:  "",
	}
	return c.call(ctx, req)
}

// DeleteTabletSnapshot deletes a snapshot from the tablet.
func (c *Client) DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	req := tabletSnapshotRequest{
		Service:    "tablet_snapshot",
		Method:     "delete_tablet_snapshot",
		Payload:    deleteTabletSnapshotRequest{SnapshotID: snapshotID, TabletID: tabletID},
		RequestID:  "",
	}
	return c.call(ctx, req)
}

// RestoreTabletSnapshot restores a snapshot on the tablet.
func (c *Client) RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	req := tabletSnapshotRequest{
		Service:    "tablet_snapshot",
		Method:     "restore_tablet_snapshot",
		Payload:    restoreTabletSnapshotRequest{SnapshotID: snapshotID, TabletID: tabletID},
		RequestID:  "",
	}
	return c.call(ctx, req)
}

// call makes an RPC call to the tablet server.
func (c *Client) call(ctx context.Context, req tabletSnapshotRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/rpc", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http status %d: %s", resp.StatusCode, string(respBody))
	}

	var rpcResp tabletSnapshotResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}

	if rpcResp.Error != nil {
		return rpcResp.Error
	}

	return nil
}

// Request/response types for tablet snapshot RPC.

type tabletSnapshotRequest struct {
	Service   string      `json:"service"`
	Method    string      `json:"method"`
	Payload   interface{} `json:"payload"`
	RequestID string      `json:"request_id,omitempty"`
}

type createTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
	CreateHT   uint64 `json:"create_ht"`
}

type deleteTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
}

type restoreTabletSnapshotRequest struct {
	SnapshotID string `json:"snapshot_id"`
	TabletID   string `json:"tablet_id"`
}

type tabletSnapshotResponse struct {
	Payload []byte        `json:"payload,omitempty"`
	Error   *dberrors.DBError `json:"error,omitempty"`
}

// TabletRPCRegistry maps tablet IDs to their RPC endpoints.
// In production this would be populated from the master's catalog.
type TabletRPCRegistry interface {
	GetEndpoint(tabletID string) (string, error)
}

// RegistryClient is a TabletSnapshotRPC implementation that uses a registry.
type RegistryClient struct {
	registry TabletRPCRegistry
}

// NewRegistryClient creates a new RegistryClient.
func NewRegistryClient(registry TabletRPCRegistry) *RegistryClient {
	return &RegistryClient{registry: registry}
}

// CreateTabletSnapshot implements TabletSnapshotRPC.
func (c *RegistryClient) CreateTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	endpoint, err := c.registry.GetEndpoint(tabletID)
	if err != nil {
		return err
	}
	client := NewClient(endpoint)
	return client.CreateTabletSnapshot(ctx, snapshotID, tabletID)
}

// DeleteTabletSnapshot implements TabletSnapshotRPC.
func (c *RegistryClient) DeleteTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	endpoint, err := c.registry.GetEndpoint(tabletID)
	if err != nil {
		return err
	}
	client := NewClient(endpoint)
	return client.DeleteTabletSnapshot(ctx, snapshotID, tabletID)
}

// RestoreTabletSnapshot implements TabletSnapshotRPC.
func (c *RegistryClient) RestoreTabletSnapshot(ctx context.Context, snapshotID, tabletID string) error {
	endpoint, err := c.registry.GetEndpoint(tabletID)
	if err != nil {
		return err
	}
	client := NewClient(endpoint)
	return client.RestoreTabletSnapshot(ctx, snapshotID, tabletID)
}
