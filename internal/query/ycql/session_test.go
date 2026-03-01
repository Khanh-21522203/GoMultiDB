package ycql

import (
	"context"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestSessionLifecycleAndDefaults(t *testing.T) {
	m := NewSessionManager()
	if err := m.OpenSession(context.Background(), "conn-1"); err != nil {
		t.Fatalf("open session: %v", err)
	}
	s, err := m.GetSession(context.Background(), "conn-1")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if s.Consistency != "ONE" {
		t.Fatalf("expected default consistency ONE, got %s", s.Consistency)
	}
	if s.SchemaVer != 1 {
		t.Fatalf("expected default schema version 1, got %d", s.SchemaVer)
	}

	if err := m.CloseSession(context.Background(), "conn-1"); err != nil {
		t.Fatalf("close session: %v", err)
	}
	if _, err := m.GetSession(context.Background(), "conn-1"); err == nil {
		t.Fatalf("expected get session to fail after close")
	}
}

func TestSessionPrepareAndExecute(t *testing.T) {
	m := NewSessionManager()
	if err := m.OpenSession(context.Background(), "conn-2"); err != nil {
		t.Fatalf("open session: %v", err)
	}
	stmt, err := m.Prepare(context.Background(), "conn-2", "SELECT * FROM ks.tbl WHERE id = ?")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if stmt.ID == "" {
		t.Fatalf("expected statement id")
	}

	resp, err := m.ExecutePrepared(context.Background(), "conn-2", stmt.ID, []Value{1})
	if err != nil {
		t.Fatalf("execute prepared: %v", err)
	}
	if !resp.Applied {
		t.Fatalf("expected applied response")
	}

	stats, err := m.PreparedStats(context.Background())
	if err != nil {
		t.Fatalf("prepared stats: %v", err)
	}
	if stats.CacheHits != 1 {
		t.Fatalf("expected cache hits 1, got %d", stats.CacheHits)
	}
	if stats.CacheMisses != 0 {
		t.Fatalf("expected cache misses 0, got %d", stats.CacheMisses)
	}
}

func TestInvalidatePreparedOnSchemaChange(t *testing.T) {
	m := NewSessionManager()
	if err := m.OpenSession(context.Background(), "conn-3"); err != nil {
		t.Fatalf("open session: %v", err)
	}
	stmt, err := m.Prepare(context.Background(), "conn-3", "SELECT now()")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if err := m.InvalidatePreparedOnSchemaChange(context.Background(), "conn-3", 2); err != nil {
		t.Fatalf("invalidate prepared: %v", err)
	}

	_, err = m.ExecutePrepared(context.Background(), "conn-3", stmt.ID, nil)
	if err == nil {
		t.Fatalf("expected execute to fail after prepared invalidation")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument for missing prepared statement, got %s", n.Code)
	}

	stats, err := m.PreparedStats(context.Background())
	if err != nil {
		t.Fatalf("prepared stats: %v", err)
	}
	if stats.InvalidationCount != 1 {
		t.Fatalf("expected invalidation count 1, got %d", stats.InvalidationCount)
	}
	if stats.CacheMisses != 1 {
		t.Fatalf("expected cache misses 1 after invalid execution, got %d", stats.CacheMisses)
	}
}

func TestPreparedConflictWhenSchemaVersionMismatches(t *testing.T) {
	m := NewSessionManager()
	if err := m.OpenSession(context.Background(), "conn-4"); err != nil {
		t.Fatalf("open session: %v", err)
	}
	stmt, err := m.Prepare(context.Background(), "conn-4", "SELECT k FROM t")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	m.mu.Lock()
	s := m.sessions["conn-4"]
	s.SchemaVer = stmt.SchemaVer + 1
	s.Prepared[stmt.ID] = stmt
	m.mu.Unlock()

	_, err = m.ExecutePrepared(context.Background(), "conn-4", stmt.ID, nil)
	if err == nil {
		t.Fatalf("expected conflict for schema mismatch")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}

	stats, err := m.PreparedStats(context.Background())
	if err != nil {
		t.Fatalf("prepared stats: %v", err)
	}
	if stats.CacheMisses != 1 {
		t.Fatalf("expected cache misses 1, got %d", stats.CacheMisses)
	}
}

func TestSessionSettersValidation(t *testing.T) {
	m := NewSessionManager()
	if err := m.OpenSession(context.Background(), "conn-5"); err != nil {
		t.Fatalf("open session: %v", err)
	}
	if err := m.SetKeyspace(context.Background(), "conn-5", "app"); err != nil {
		t.Fatalf("set keyspace: %v", err)
	}
	if err := m.SetConsistency(context.Background(), "conn-5", "QUORUM"); err != nil {
		t.Fatalf("set consistency: %v", err)
	}

	s, err := m.GetSession(context.Background(), "conn-5")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if s.Keyspace != "app" {
		t.Fatalf("expected keyspace app, got %s", s.Keyspace)
	}
	if s.Consistency != "QUORUM" {
		t.Fatalf("expected consistency QUORUM, got %s", s.Consistency)
	}
}
