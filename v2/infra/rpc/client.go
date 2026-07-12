package rpc

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/types"
)

// ClientConfig configures a Client's target base URL and transport
// behavior.
type ClientConfig struct {
	BaseURL string
	Timeout time.Duration
	TLS     *tls.Config
}

// Client issues JSON-RPC calls over HTTP to a remote Server.
//
// Scaffold stub: Call returns errors.ErrNotImplemented.
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// NewClient returns a Client configured to call cfg.BaseURL, applying
// cfg.Timeout and cfg.TLS to the underlying HTTP transport. Returns an
// error if cfg.BaseURL is empty.
func NewClient(cfg ClientConfig) (*Client, error) {
	return &Client{}, nil
}

// Call invokes the given service/method pair on the remote peer, sending
// envelope and payload as the request body, and returns the raw response
// payload.
//
// Not yet implemented in this scaffold.
func (c *Client) Call(ctx context.Context, envelope types.RequestEnvelope, service, method string, payload []byte) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}
