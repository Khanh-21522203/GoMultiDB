package rocks

import (
	"bytes"
	"context"
	"sort"
	"sync"
)

type DBKind int

const (
	RegularDB DBKind = iota
	IntentsDB
)

type KV struct {
	Key   []byte
	Value []byte
}

type WriteBatch struct {
	Ops []KV
}

type Iterator interface {
	Next() bool
	Item() KV
	Err() error
}

type Store interface {
	ApplyWriteBatch(ctx context.Context, kind DBKind, wb WriteBatch) error
	Get(ctx context.Context, kind DBKind, key []byte) ([]byte, bool, error)
	NewIterator(ctx context.Context, kind DBKind, prefix []byte) (Iterator, error)
}

type MemoryStore struct {
	mu      sync.RWMutex
	regular map[string][]byte
	intents map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		regular: make(map[string][]byte),
		intents: make(map[string][]byte),
	}
}

func (s *MemoryStore) ApplyWriteBatch(_ context.Context, kind DBKind, wb WriteBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	target := s.pick(kind)
	for _, op := range wb.Ops {
		if op.Value == nil {
			delete(target, string(op.Key))
			continue
		}
		target[string(op.Key)] = append([]byte(nil), op.Value...)
	}
	return nil
}

func (s *MemoryStore) Get(_ context.Context, kind DBKind, key []byte) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.pick(kind)[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), v...), true, nil
}

func (s *MemoryStore) NewIterator(_ context.Context, kind DBKind, prefix []byte) (Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]KV, 0)
	for k, v := range s.pick(kind) {
		if len(prefix) > 0 && !bytes.HasPrefix([]byte(k), prefix) {
			continue
		}
		items = append(items, KV{Key: []byte(k), Value: append([]byte(nil), v...)})
	}
	sort.Slice(items, func(i, j int) bool { return bytes.Compare(items[i].Key, items[j].Key) < 0 })
	return &memoryIter{items: items, idx: -1}, nil
}

func (s *MemoryStore) pick(kind DBKind) map[string][]byte {
	if kind == IntentsDB {
		return s.intents
	}
	return s.regular
}

type memoryIter struct {
	items []KV
	idx   int
}

func (i *memoryIter) Next() bool {
	i.idx++
	return i.idx < len(i.items)
}

func (i *memoryIter) Item() KV {
	if i.idx < 0 || i.idx >= len(i.items) {
		return KV{}
	}
	return i.items[i.idx]
}

func (i *memoryIter) Err() error { return nil }
