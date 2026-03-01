package ycql

import (
	"context"
	"fmt"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
)

type PreparedStmt struct {
	ID        string
	Query     string
	Plan      []byte
	SchemaVer uint64
}

type PreparedStats struct {
	CacheHits         uint64
	CacheMisses       uint64
	InvalidationCount uint64
}

type Session struct {
	ConnID      string
	Prepared    map[string]PreparedStmt
	Keyspace    string
	Consistency string
	SchemaVer   uint64
}

type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	stats    PreparedStats
}

func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*Session)}
}

func (m *SessionManager) OpenSession(ctx context.Context, connID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if connID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "connection id is required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[connID]; ok {
		return nil
	}
	m.sessions[connID] = &Session{
		ConnID:      connID,
		Prepared:    make(map[string]PreparedStmt),
		Consistency: "ONE",
		SchemaVer:   1,
	}
	return nil
}

func (m *SessionManager) CloseSession(ctx context.Context, connID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if connID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "connection id is required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, connID)
	return nil
}

func (m *SessionManager) GetSession(ctx context.Context, connID string) (*Session, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[connID]
	if !ok {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	cp := *s
	cp.Prepared = clonePrepared(s.Prepared)
	return &cp, nil
}

func (m *SessionManager) SetKeyspace(ctx context.Context, connID, keyspace string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if connID == "" || keyspace == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "connection id and keyspace are required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[connID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	s.Keyspace = keyspace
	return nil
}

func (m *SessionManager) SetConsistency(ctx context.Context, connID, consistency string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if connID == "" || consistency == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "connection id and consistency are required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[connID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	s.Consistency = consistency
	return nil
}

func (m *SessionManager) Prepare(ctx context.Context, connID, query string) (PreparedStmt, error) {
	select {
	case <-ctx.Done():
		return PreparedStmt{}, ctx.Err()
	default:
	}
	if connID == "" || query == "" {
		return PreparedStmt{}, dberrors.New(dberrors.ErrInvalidArgument, "connection id and query are required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[connID]
	if !ok {
		return PreparedStmt{}, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}

	id := fmt.Sprintf("%s-ps-%d", connID, len(s.Prepared)+1)
	stmt := PreparedStmt{ID: id, Query: query, Plan: []byte(query), SchemaVer: s.SchemaVer}
	s.Prepared[id] = stmt
	return stmt, nil
}

func (m *SessionManager) ExecutePrepared(ctx context.Context, connID, stmtID string, _ []Value) (Response, error) {
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	default:
	}
	if connID == "" || stmtID == "" {
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "connection id and statement id are required", false, nil)
	}

	m.mu.RLock()
	s, ok := m.sessions[connID]
	if !ok {
		m.mu.RUnlock()
		m.incPreparedMiss()
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	stmt, ok := s.Prepared[stmtID]
	schemaVer := s.SchemaVer
	m.mu.RUnlock()
	if !ok {
		m.incPreparedMiss()
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "prepared statement not found", false, nil)
	}
	if stmt.SchemaVer != schemaVer {
		m.incPreparedMiss()
		return Response{}, dberrors.New(dberrors.ErrConflict, "prepared statement invalid due to schema change", true, nil)
	}
	m.incPreparedHit()
	return Response{Applied: true, Rows: 0}, nil
}

func (m *SessionManager) InvalidatePreparedOnSchemaChange(ctx context.Context, connID string, newSchemaVer uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if connID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "connection id is required", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[connID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if newSchemaVer <= s.SchemaVer {
		return nil
	}
	s.SchemaVer = newSchemaVer
	s.Prepared = make(map[string]PreparedStmt)
	m.stats.InvalidationCount++
	return nil
}

func (m *SessionManager) PreparedStats(ctx context.Context) (PreparedStats, error) {
	select {
	case <-ctx.Done():
		return PreparedStats{}, ctx.Err()
	default:
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stats, nil
}

func (m *SessionManager) incPreparedHit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats.CacheHits++
}

func (m *SessionManager) incPreparedMiss() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats.CacheMisses++
}

func clonePrepared(in map[string]PreparedStmt) map[string]PreparedStmt {
	out := make(map[string]PreparedStmt, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
