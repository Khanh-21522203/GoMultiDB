package rocks_test

import (
	"context"
	"testing"

	"GoMultiDB/internal/storage/rocks"
)

func TestMemoryStoreWriteReadIter(t *testing.T) {
	s := rocks.NewMemoryStore()
	ctx := context.Background()
	if err := s.ApplyWriteBatch(ctx, rocks.RegularDB, rocks.WriteBatch{Ops: []rocks.KV{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: []byte("2")}}}); err != nil {
		t.Fatalf("apply batch: %v", err)
	}
	v, ok, err := s.Get(ctx, rocks.RegularDB, []byte("a"))
	if err != nil || !ok || string(v) != "1" {
		t.Fatalf("unexpected get result ok=%v val=%s err=%v", ok, string(v), err)
	}
	it, err := s.NewIterator(ctx, rocks.RegularDB, []byte(""))
	if err != nil {
		t.Fatalf("new iterator: %v", err)
	}
	count := 0
	for it.Next() {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 items, got %d", count)
	}
}
