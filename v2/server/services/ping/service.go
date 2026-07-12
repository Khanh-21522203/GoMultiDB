package ping

import (
	"context"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/types"

	rpcpkg "GoMultiDB/v2/infra/rpc"
)

// Request is the payload of a ping "echo" call.
type Request struct {
	Message string `json:"message"`
}

// Response is the result of a ping "echo" call.
type Response struct {
	Message   string    `json:"message"`
	ServerAt  time.Time `json:"server_at"`
	Source    string    `json:"source"`
	RequestID string    `json:"request_id"`
}

// Service exposes a single "echo" method via JSON-RPC, used to verify
// node-to-node connectivity.
//
// Scaffold stub: Name and Methods panic.
type Service struct {
	nodeName string
}

// NewService creates a new ping service reporting nodeName as its Source.
func NewService(nodeName string) *Service {
	return &Service{}
}

// Name returns the service name for RPC registration.
//
// Not yet implemented in this scaffold.
func (s *Service) Name() string {
	panic("not implemented")
}

// Methods returns the RPC method handlers.
//
// Not yet implemented in this scaffold.
func (s *Service) Methods() map[string]rpcpkg.HandlerFunc {
	panic("not implemented")
}

// echo handles the "echo" RPC method: it echoes back the request message
// (defaulting to "pong" when empty), stamped with the current time,
// s.nodeName, and the request envelope's ID.
func (s *Service) echo(_ context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {
	return nil, errors.ErrNotImplemented
}
