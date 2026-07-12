package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/types"
)

// Config configures a Server's bind address and HTTP transport behavior.
type Config struct {
	BindAddress         string
	ReadHeaderTimeout   time.Duration
	StrictContractCheck bool
	TLSConfig           *tls.Config
}

// HandlerFunc handles a single decoded RPC request and returns the raw
// response payload.
type HandlerFunc func(ctx context.Context, envelope types.RequestEnvelope, payload []byte) ([]byte, error)

// Service is a named collection of RPC methods that a Server can dispatch
// requests to.
type Service interface {
	// Name returns the service's registration name.
	Name() string
	// Methods returns the service's method table, keyed by method name.
	Methods() map[string]HandlerFunc
}

// Server is an HTTP-based JSON-RPC server that dispatches incoming
// requests to registered Service implementations by service and method
// name, validating each request's contract version before dispatch.
//
// Scaffold stub: RegisterService, Start, and Stop return
// errors.ErrNotImplemented.
type Server struct {
	cfg Config

	mu       sync.RWMutex
	services map[string]Service

	httpSrv  *http.Server
	listener net.Listener
}

// NewServer returns a Server configured to bind to cfg.BindAddress.
// Returns an error if cfg.BindAddress is empty.
func NewServer(cfg Config) (*Server, error) {
	return &Server{}, nil
}

// RegisterService registers svc under its own name. Returns an error if
// svc is nil, its name is empty, or a service is already registered under
// that name.
//
// Not yet implemented in this scaffold.
func (s *Server) RegisterService(svc Service) error {
	return errors.ErrNotImplemented
}

// GetBindAddress returns the address the server is configured to bind to.
//
// Not yet implemented in this scaffold.
func (s *Server) GetBindAddress() string {
	panic("not implemented")
}

// Start begins accepting and dispatching RPC requests on the configured
// bind address.
//
// Not yet implemented in this scaffold.
func (s *Server) Start(ctx context.Context) error {
	return errors.ErrNotImplemented
}

// Stop gracefully shuts down the server, waiting for in-flight requests to
// complete or ctx to be done.
//
// Not yet implemented in this scaffold.
func (s *Server) Stop(ctx context.Context) error {
	return errors.ErrNotImplemented
}
