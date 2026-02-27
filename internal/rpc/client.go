package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/types"
)

type ClientConfig struct {
	BaseURL string
	Timeout time.Duration
	TLS     *tls.Config
}

type Client struct {
	httpClient *http.Client
	baseURL    string
}

func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("base url is required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	tr := &http.Transport{TLSClientConfig: cfg.TLS}
	return &Client{
		httpClient: &http.Client{Timeout: cfg.Timeout, Transport: tr},
		baseURL:    cfg.BaseURL,
	}, nil
}

func (c *Client) Call(ctx context.Context, envelope types.RequestEnvelope, service, method string, payload []byte) ([]byte, error) {
	reqBody := rpcRequest{
		Envelope: envelope,
		Service:  service,
		Method:   method,
		Payload:  payload,
	}
	b, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, dberrors.New(dberrors.ErrRetryableUnavailable, "rpc call failed", true, err)
	}
	defer resp.Body.Close()

	var out rpcResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Error != nil {
		return nil, *out.Error
	}
	return out.Payload, nil
}
