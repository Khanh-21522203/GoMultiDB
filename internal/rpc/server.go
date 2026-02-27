package rpc

import (
	"context"

	"crypto/tls"

	"fmt"

	"net"

	"net/http"

	"sync"

	"time"

	dberrors "GoMultiDB/internal/common/errors"

	"GoMultiDB/internal/common/types"

	"GoMultiDB/internal/common/versioning"
)

type Config struct {
	BindAddress string

	ReadHeaderTimeout time.Duration

	StrictContractCheck bool

	TLSConfig *tls.Config
}

type HandlerFunc func(ctx context.Context, envelope types.RequestEnvelope, payload []byte) ([]byte, error)

type Service interface {
	Name() string

	Methods() map[string]HandlerFunc
}

type Server struct {
	cfg Config

	mu sync.RWMutex

	services map[string]Service

	httpSrv *http.Server

	listener net.Listener
}

func NewServer(cfg Config) (*Server, error) {

	if cfg.BindAddress == "" {

		return nil, fmt.Errorf("rpc bind address is required")

	}

	if cfg.ReadHeaderTimeout == 0 {

		cfg.ReadHeaderTimeout = 5 * time.Second

	}

	return &Server{cfg: cfg, services: make(map[string]Service)}, nil

}

func (s *Server) RegisterService(svc Service) error {

	if svc == nil {

		return fmt.Errorf("service is nil")

	}

	if svc.Name() == "" {

		return fmt.Errorf("service name is empty")

	}

	s.mu.Lock()

	defer s.mu.Unlock()

	if _, exists := s.services[svc.Name()]; exists {

		return fmt.Errorf("service already registered: %s", svc.Name())

	}

	s.services[svc.Name()] = svc

	return nil

}

func (s *Server) Start(ctx context.Context) error {

	s.mu.Lock()

	defer s.mu.Unlock()

	if s.httpSrv != nil {

		return nil

	}

	mux := http.NewServeMux()

	mux.HandleFunc("/rpc", s.handleRPC)

	s.httpSrv = &http.Server{

		Addr: s.cfg.BindAddress,

		Handler: mux,

		ReadHeaderTimeout: s.cfg.ReadHeaderTimeout,

		TLSConfig: s.cfg.TLSConfig,
	}

	ln, err := net.Listen("tcp", s.cfg.BindAddress)

	if err != nil {

		s.httpSrv = nil

		return fmt.Errorf("listen rpc: %w", err)

	}

	s.listener = ln

	go func() {

		_ = s.httpSrv.Serve(ln)

	}()

	select {

	case <-ctx.Done():

		return ctx.Err()

	default:

		return nil

	}

}

func (s *Server) Stop(ctx context.Context) error {

	s.mu.Lock()

	h := s.httpSrv

	s.httpSrv = nil

	s.listener = nil

	s.mu.Unlock()

	if h == nil {

		return nil

	}

	return h.Shutdown(ctx)

}

type rpcRequest struct {
	Envelope types.RequestEnvelope `json:"envelope"`

	Service string `json:"service"`

	Method string `json:"method"`

	Payload []byte `json:"payload"`
}

type rpcResponse struct {
	Payload []byte `json:"payload,omitempty"`

	Error *dberrors.DBError `json:"error,omitempty"`
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return

	}

	var req rpcRequest

	if err := decodeJSON(r, &req); err != nil {

		writeJSON(w, http.StatusBadRequest, rpcResponse{Error: ptr(dberrors.New(dberrors.ErrInvalidArgument, "invalid rpc request", false, err))})

		return

	}

	if err := versioning.ValidateContractVersion(req.Envelope.ContractVer, s.cfg.StrictContractCheck); err != nil {

		n := dberrors.NormalizeError(err)

		writeJSON(w, http.StatusBadRequest, rpcResponse{Error: &n})

		return

	}

	s.mu.RLock()

	svc, ok := s.services[req.Service]

	s.mu.RUnlock()

	if !ok {

		e := dberrors.New(dberrors.ErrInvalidArgument, "unknown service", false, nil)

		writeJSON(w, http.StatusBadRequest, rpcResponse{Error: &e})

		return

	}

	method, ok := svc.Methods()[req.Method]

	if !ok {

		e := dberrors.New(dberrors.ErrInvalidArgument, "unknown service method", false, nil)

		writeJSON(w, http.StatusBadRequest, rpcResponse{Error: &e})

		return

	}

	resp, err := method(r.Context(), req.Envelope, req.Payload)

	if err != nil {

		n := dberrors.NormalizeError(err)

		writeJSON(w, http.StatusOK, rpcResponse{Error: &n})

		return

	}

	writeJSON(w, http.StatusOK, rpcResponse{Payload: resp})

}

func ptr[T any](v T) *T { return &v }
