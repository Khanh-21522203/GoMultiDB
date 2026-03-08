package cql

import (
	"context"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
)

type Config struct {
	Enabled        bool
	BindAddress    string
	MaxConnections int
}

type Request struct {
	ConnID     string
	Query      string
	PreparedID string
	Vars       []any
}

type Response struct {
	Applied bool
	Rows    int
}

type Status struct {
	Started           bool
	MaxConnections    int
	ActiveConnections int
	Prepared          PreparedStats
}

type Server interface {
	Start(ctx context.Context, cfg Config) error
	Stop(ctx context.Context) error
	Health(ctx context.Context) error
	Route(ctx context.Context, req Request) (Response, error)
	RouteBatch(ctx context.Context, req any) (Response, error)
}

type LocalServer struct {
	mu          sync.RWMutex
	started     bool
	cfg         Config
	sessions    *SessionManager
	activeConns map[string]struct{}
	listener    *Listener
}

func NewLocalServer() *LocalServer {
	s := &LocalServer{sessions: NewSessionManager(), activeConns: make(map[string]struct{})}
	s.listener = NewListener(s)
	return s
}

func (s *LocalServer) Start(ctx context.Context, cfg Config) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if cfg.Enabled {
		if cfg.BindAddress == "" {
			return dberrors.New(dberrors.ErrInvalidArgument, "cql bind address is required when enabled", false, nil)
		}
		if cfg.MaxConnections <= 0 {
			cfg.MaxConnections = 1000
		}
		// Start the protocol listener.
		if err := s.listener.Start(ctx, cfg); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		if s.cfg == cfg {
			return nil
		}
		return dberrors.New(dberrors.ErrConflict, "cql server already started with different config", false, nil)
	}
	s.started = true
	s.cfg = cfg
	return nil
}

func (s *LocalServer) Stop(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// Stop listener first.
	if err := s.listener.Stop(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = false
	s.cfg = Config{}
	s.sessions = NewSessionManager()
	s.activeConns = make(map[string]struct{})
	return nil
}

func (s *LocalServer) Health(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started {
		return dberrors.New(dberrors.ErrRetryableUnavailable, "cql is not started", true, nil)
	}
	return nil
}

func (s *LocalServer) Status(ctx context.Context) (Status, error) {
	select {
	case <-ctx.Done():
		return Status{}, ctx.Err()
	default:
	}
	s.mu.RLock()
	started := s.started
	maxConns := s.cfg.MaxConnections
	active := len(s.activeConns)
	s.mu.RUnlock()

	stats, err := s.sessions.PreparedStats(ctx)
	if err != nil {
		return Status{}, err
	}
	return Status{Started: started, MaxConnections: maxConns, ActiveConnections: active, Prepared: stats}, nil
}

func (s *LocalServer) OpenConnection(ctx context.Context, connID string) error {
	if err := s.Health(ctx); err != nil {
		return err
	}
	if connID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "connection id is required", false, nil)
	}

	s.mu.Lock()
	if _, ok := s.activeConns[connID]; ok {
		s.mu.Unlock()
		return nil
	}
	if s.cfg.MaxConnections > 0 && len(s.activeConns) >= s.cfg.MaxConnections {
		s.mu.Unlock()
		return dberrors.New(dberrors.ErrRetryableUnavailable, "cql max connections reached", true, nil)
	}
	s.activeConns[connID] = struct{}{}
	s.mu.Unlock()

	if err := s.sessions.OpenSession(ctx, connID); err != nil {
		s.mu.Lock()
		delete(s.activeConns, connID)
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *LocalServer) CloseConnection(ctx context.Context, connID string) error {
	if err := s.Health(ctx); err != nil {
		return err
	}
	if connID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "connection id is required", false, nil)
	}

	s.mu.Lock()
	delete(s.activeConns, connID)
	s.mu.Unlock()
	return s.sessions.CloseSession(ctx, connID)
}

func (s *LocalServer) Prepare(ctx context.Context, connID, query string) (PreparedStmt, error) {
	if err := s.Health(ctx); err != nil {
		return PreparedStmt{}, err
	}
	if err := s.OpenConnection(ctx, connID); err != nil {
		return PreparedStmt{}, err
	}
	return s.sessions.Prepare(ctx, connID, query)
}

func (s *LocalServer) Route(ctx context.Context, req Request) (Response, error) {
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	default:
	}
	if err := s.Health(ctx); err != nil {
		return Response{}, err
	}

	if req.PreparedID != "" {
		if req.ConnID == "" {
			return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "connection id is required for prepared execution", false, nil)
		}
		if err := s.OpenConnection(ctx, req.ConnID); err != nil {
			return Response{}, err
		}
		return s.sessions.ExecutePrepared(ctx, req.ConnID, req.PreparedID, req.Vars)
	}

	if req.Query == "" {
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "query is required", false, nil)
	}
	if req.ConnID != "" {
		if err := s.OpenConnection(ctx, req.ConnID); err != nil {
			return Response{}, err
		}
	}
	return Response{Applied: true, Rows: 0}, nil
}

func (s *LocalServer) RouteBatch(ctx context.Context, reqAny any) (Response, error) {
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	default:
	}
	if err := s.Health(ctx); err != nil {
		return Response{}, err
	}
	// For now, just acknowledge batch requests.
	return Response{Applied: true, Rows: 0}, nil
}
