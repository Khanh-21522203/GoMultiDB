package wal_test

import (
	"context"

	"testing"

	"time"

	"GoMultiDB/internal/common/types"

	"GoMultiDB/internal/wal"
)

func TestLogAppendAndRecover(t *testing.T) {

	dir := t.TempDir()

	l, err := wal.NewLog(wal.Config{Dir: dir})

	if err != nil {

		t.Fatalf("new log: %v", err)

	}

	entries := []wal.Entry{

		{OpID: types.OpID{Term: 1, Index: 1}, HybridTime: 10, Payload: []byte("a")},

		{OpID: types.OpID{Term: 1, Index: 2}, HybridTime: 11, Payload: []byte("b")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	defer cancel()

	if err := l.AppendSync(ctx, entries); err != nil {

		t.Fatalf("append sync: %v", err)

	}

	if got := l.LastOpID(); !got.Equal(types.OpID{Term: 1, Index: 2}) {

		t.Fatalf("last op mismatch: %+v", got)

	}

	if err := l.Close(); err != nil {

		t.Fatalf("close: %v", err)

	}

	l2, err := wal.NewLog(wal.Config{Dir: dir})

	if err != nil {

		t.Fatalf("reopen log: %v", err)

	}

	defer l2.Close()

	got := l2.ReadFrom(types.OpID{}, 10)

	if len(got) != 2 {

		t.Fatalf("expected 2 entries, got %d", len(got))

	}

	if string(got[1].Payload) != "b" {

		t.Fatalf("unexpected payload: %s", string(got[1].Payload))

	}

}

func TestLogRotateAndIndexLookup(t *testing.T) {

	dir := t.TempDir()

	l, err := wal.NewLog(wal.Config{Dir: dir, MaxSegmentBytes: 64})

	if err != nil {

		t.Fatalf("new log: %v", err)

	}

	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	defer cancel()

	for i := 1; i <= 10; i++ {

		e := wal.Entry{OpID: types.OpID{Term: 1, Index: uint64(i)}, Payload: []byte("payload-123456789")}

		if err := l.AppendSync(ctx, []wal.Entry{e}); err != nil {

			t.Fatalf("append %d: %v", i, err)

		}

	}

	seg, off, ok := l.IndexLookup(types.OpID{Term: 1, Index: 10})

	if !ok {

		t.Fatalf("expected index entry for op 10")

	}

	if seg == "" {

		t.Fatalf("expected non-empty segment name")

	}

	if off < 0 {

		t.Fatalf("expected non-negative offset")

	}

}
