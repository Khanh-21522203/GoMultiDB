package cql

import (
	"context"
	"strings"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
)

type statementKind int

const (
	stmtSelect statementKind = iota + 1
	stmtInsert
	stmtUpdate
	stmtDelete
)

type statement struct {
	kind  statementKind
	table string
}

type executionEngine struct {
	mu       sync.RWMutex
	tableRow map[string]int
}

func newExecutionEngine() *executionEngine {
	return &executionEngine{
		tableRow: make(map[string]int),
	}
}

func (e *executionEngine) Execute(ctx context.Context, query string, _ []any) (Response, error) {
	select {
	case <-ctx.Done():
		return Response{}, ctx.Err()
	default:
	}

	stmt, err := parseStatement(query)
	if err != nil {
		return Response{}, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	switch stmt.kind {
	case stmtSelect:
		rows := e.tableRow[stmt.table]
		if rows == 0 {
			rows = 1
		}
		return Response{Applied: true, Rows: rows}, nil
	case stmtInsert:
		e.tableRow[stmt.table]++
		return Response{Applied: true, Rows: 0}, nil
	case stmtUpdate:
		// Preserve update semantics for rows that do not exist yet.
		if e.tableRow[stmt.table] == 0 {
			e.tableRow[stmt.table] = 1
		}
		return Response{Applied: true, Rows: 0}, nil
	case stmtDelete:
		if e.tableRow[stmt.table] > 0 {
			e.tableRow[stmt.table]--
		}
		return Response{Applied: true, Rows: 0}, nil
	default:
		return Response{}, dberrors.New(dberrors.ErrInvalidArgument, "unsupported cql statement", false, nil)
	}
}

func parseStatement(query string) (statement, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if trimmed == "" {
		return statement{}, dberrors.New(dberrors.ErrInvalidArgument, "query is required", false, nil)
	}
	upper := strings.ToUpper(trimmed)

	switch {
	case strings.HasPrefix(upper, "SELECT "):
		table := parseTableAfterKeyword(trimmed, upper, " FROM ")
		if table == "" {
			table = "__select_expr__"
		}
		return statement{kind: stmtSelect, table: table}, nil
	case strings.HasPrefix(upper, "INSERT INTO "):
		table := parseIdentifierAfterPrefix(trimmed, len("INSERT INTO "))
		if table == "" {
			return statement{}, dberrors.New(dberrors.ErrInvalidArgument, "INSERT requires table name", false, nil)
		}
		return statement{kind: stmtInsert, table: table}, nil
	case strings.HasPrefix(upper, "UPDATE "):
		table := parseIdentifierAfterPrefix(trimmed, len("UPDATE "))
		if table == "" {
			return statement{}, dberrors.New(dberrors.ErrInvalidArgument, "UPDATE requires table name", false, nil)
		}
		return statement{kind: stmtUpdate, table: table}, nil
	case strings.HasPrefix(upper, "DELETE "):
		table := parseTableAfterKeyword(trimmed, upper, " FROM ")
		if table == "" {
			return statement{}, dberrors.New(dberrors.ErrInvalidArgument, "DELETE requires FROM <table>", false, nil)
		}
		return statement{kind: stmtDelete, table: table}, nil
	default:
		return statement{}, dberrors.New(dberrors.ErrInvalidArgument, "unsupported cql statement type", false, nil)
	}
}

func parseTableAfterKeyword(original, upper, keyword string) string {
	idx := strings.Index(upper, keyword)
	if idx < 0 {
		return ""
	}
	return parseIdentifierAfterPrefix(original, idx+len(keyword))
}

func parseIdentifierAfterPrefix(query string, start int) string {
	if start < 0 || start >= len(query) {
		return ""
	}
	rest := strings.TrimSpace(query[start:])
	if rest == "" {
		return ""
	}
	end := 0
	for end < len(rest) {
		ch := rest[end]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '.' || ch == '"' {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return ""
	}
	return strings.Trim(rest[:end], "\"")
}
