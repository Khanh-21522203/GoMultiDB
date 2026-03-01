package ycql

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
	Vars       []Value
}

type BatchType string

const (
	BatchTypeLogged   BatchType = "LOGGED"
	BatchTypeUnlogged BatchType = "UNLOGGED"
)

type BatchRequest struct {
	Type       BatchType
	Statements []Request
}

type Value any

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
	RouteBatch(ctx context.Context, req BatchRequest) (Response, error)
}

type LocalServer struct {
	mu          sync.RWMutex
	started     bool
	cfg         Config
	sessions    *SessionManager
	activeConns map[string]struct{}
}

func NewLocalServer() *LocalServer {
	return &LocalServer{sessions: NewSessionManager(), activeConns: make(map[string]struct{})}
}

func (s *LocalServer) Start(ctx context.Context, cfg Config) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if cfg.Enabled {
		if cfg.BindAddress == "" {
			return dberrors.New(dberrors.ErrInvalidArgument, "ycql bind address is required when enabled", false, nil)
		}
		if cfg.MaxConnections <= 0 {
			cfg.MaxConnections = 1000
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		if s.cfg == cfg {
			return nil
		}
		return dberrors.New(dberrors.ErrConflict, "ycql server already started with different config", false, nil)
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
		return dberrors.New(dberrors.ErrRetryableUnavailable, "ycql is not started", true, nil)
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
		return dberrors.New(dberrors.ErrRetryableUnavailable, "ycql max connections reached", true, nil)
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

func (s *LocalServer) RouteBatch(ctx context.Context, req BatchRequest) (Response, error) {
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	default:
	}
	if err := s.Health(ctx); err != nil {
		return Response{}, err
	}
	if req.Type == "" {
		req.Type = BatchTypeLogged
	}
	if req.Type != BatchTypeLogged && req.Type != BatchTypeUnlogged {
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "invalid batch type", false, nil)
	}
	if len(req.Statements) == 0 {
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "batch statements are required", false, nil)
	}
	for _, st := range req.Statements {
		if st.PreparedID != "" {
			if st.ConnID == "" {
				return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "connection id is required for prepared statement in batch", false, nil)
			}
			if err := s.OpenConnection(ctx, st.ConnID); err != nil {
				return Response{}, err
			}
			if _, err := s.sessions.ExecutePrepared(ctx, st.ConnID, st.PreparedID, st.Vars); err != nil {
				return Response{}, err
			}
			continue
		}
		if st.Query == "" {
			return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "batch statement query is required", false, nil)
		}
		if st.ConnID != "" {
			if err := s.OpenConnection(ctx, st.ConnID); err != nil {
				return Response{}, err
			}
		}
	}
	return Response{Applied: true, Rows: len(req.Statements)}, nil
}
