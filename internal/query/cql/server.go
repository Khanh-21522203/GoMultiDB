package cql

import (
	"context"
	"fmt"
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
	executor    *executionEngine
	activeConns map[string]struct{}
	listener    *Listener
}

func NewLocalServer() *LocalServer {
	s := &LocalServer{
		sessions:    NewSessionManager(),
		executor:    newExecutionEngine(),
		activeConns: make(map[string]struct{}),
	}
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
	s.executor = newExecutionEngine()
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
		stmt, err := s.sessions.ResolvePrepared(ctx, req.ConnID, req.PreparedID)
		if err != nil {
			return Response{}, err
		}
		return s.executor.Execute(ctx, stmt.Query, req.Vars)
	}

	if req.Query == "" {
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "query is required", false, nil)
	}
	if req.ConnID != "" {
		if err := s.OpenConnection(ctx, req.ConnID); err != nil {
			return Response{}, err
		}
	}
	return s.executor.Execute(ctx, req.Query, req.Vars)
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
	requests, err := normalizeBatchRequests(reqAny)
	if err != nil {
		return Response{}, err
	}
	totalRows := 0
	for _, req := range requests {
		resp, routeErr := s.Route(ctx, req)
		if routeErr != nil {
			return Response{}, routeErr
		}
		totalRows += resp.Rows
	}
	return Response{Applied: true, Rows: totalRows}, nil
}

func normalizeBatchRequests(reqAny any) ([]Request, error) {
	switch req := reqAny.(type) {
	case map[string]any:
		connID, _ := req["conn_id"].(string)

		if statements, ok := req["statements"].([]Request); ok {
			out := make([]Request, 0, len(statements))
			for _, st := range statements {
				if st.ConnID == "" {
					st.ConnID = connID
				}
				out = append(out, st)
			}
			if len(out) == 0 {
				return nil, dberrors.New(dberrors.ErrInvalidArgument, "batch statements are required", false, nil)
			}
			return out, nil
		}

		if queries, ok := req["queries"].([]BatchQuery); ok {
			return convertBatchQueries(connID, queries)
		}

		return nil, dberrors.New(dberrors.ErrInvalidArgument, "batch request must provide statements or queries", false, nil)
	case BatchRequest:
		return convertBatchQueries("", req.Queries)
	case []Request:
		if len(req) == 0 {
			return nil, dberrors.New(dberrors.ErrInvalidArgument, "batch statements are required", false, nil)
		}
		return req, nil
	default:
		return nil, dberrors.New(dberrors.ErrInvalidArgument, fmt.Sprintf("unsupported batch request type %T", reqAny), false, nil)
	}
}

func convertBatchQueries(connID string, queries []BatchQuery) ([]Request, error) {
	if len(queries) == 0 {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "batch queries are required", false, nil)
	}
	out := make([]Request, 0, len(queries))
	for _, q := range queries {
		req := Request{ConnID: connID}
		if q.Kind == 0 {
			req.Query = q.QueryString
		} else if q.Kind == 1 {
			req.PreparedID = string(q.QueryID)
		} else {
			return nil, dberrors.New(dberrors.ErrInvalidArgument, "unsupported batch query kind", false, nil)
		}
		if len(q.Values) > 0 {
			req.Vars = make([]any, len(q.Values))
			for i, v := range q.Values {
				req.Vars[i] = v
			}
		}
		out = append(out, req)
	}
	return out, nil
}
