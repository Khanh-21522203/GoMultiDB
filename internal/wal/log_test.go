package wal_test

import (
	"context"
	"os"
	"path/filepath"
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

	l2, err := wal.NewLog(wal.Config{Dir: dir, SegmentFile: filepath.Base("segment-000001.wal")})
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
	l, err := wal.NewLog(wal.Config{Dir: dir, MaxSegmentBytes: 80})
	if err != nil {
		t.Fatalf("new log: %v", err)
	}
	defer l.Close()

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		e := wal.Entry{OpID: types.OpID{Term: 1, Index: uint64(i)}, Payload: []byte("payload-for-rotation")}
		if err := l.AppendSync(ctx, []wal.Entry{e}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	seg, off, ok := l.IndexLookup(types.OpID{Term: 1, Index: 5})
	if !ok || seg == "" || off < 0 {
		t.Fatalf("index lookup failed: ok=%v seg=%s off=%d", ok, seg, off)
	}
}

func TestRecoverTruncatesCorruptTail(t *testing.T) {
	dir := t.TempDir()
	l, err := wal.NewLog(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("new log: %v", err)
	}
	if err := l.AppendSync(context.Background(), []wal.Entry{{OpID: types.OpID{Term: 1, Index: 1}, Payload: []byte("ok")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, "segment-000001.wal"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.WriteString("{bad-json-record\n"); err != nil {
		_ = f.Close()
		t.Fatalf("append corruption: %v", err)
	}
	_ = f.Close()

	l2, err := wal.NewLog(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	defer l2.Close()
	all := l2.ReadFrom(types.OpID{}, 10)
	if len(all) != 1 || string(all[0].Payload) != "ok" {
		t.Fatalf("unexpected recovered entries: %+v", all)
	}
}
