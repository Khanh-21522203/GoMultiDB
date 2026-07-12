package rocks

import (
	"context"
	"sync"

	errors "GoMultiDB/v2/contracts/errors"
)

// DBKind selects which logical database an operation targets.
type DBKind int

// DBKind values: RegularDB holds committed table data, IntentsDB holds
// provisional (uncommitted) transaction writes.
const (
	RegularDB DBKind = iota
	IntentsDB
)

// KV is a single key-value pair. A nil Value denotes a deletion when used
// in a WriteBatch.
type KV struct {
	Key   []byte
	Value []byte
}

// WriteBatch is an ordered set of key-value operations applied atomically.
type WriteBatch struct {
	Ops []KV
}

// Iterator iterates over key-value pairs in key order.
type Iterator interface {
	// Next advances the iterator and reports whether an item is
	// available.
	Next() bool
	// Item returns the current key-value pair.
	Item() KV
	// Err returns the first error encountered during iteration, if any.
	Err() error
}

// Store is an ordered key-value store split into two logical databases,
// selected by DBKind.
type Store interface {
	// ApplyWriteBatch atomically applies wb to the database selected by
	// kind.
	ApplyWriteBatch(ctx context.Context, kind DBKind, wb WriteBatch) error
	// Get returns the value stored under key in the database selected by
	// kind, and whether it was found.
	Get(ctx context.Context, kind DBKind, key []byte) ([]byte, bool, error)
	// NewIterator returns an Iterator over keys with the given prefix in
	// the database selected by kind, in key order.
	NewIterator(ctx context.Context, kind DBKind, prefix []byte) (Iterator, error)
}

// MemoryStore is an in-memory, non-durable reference implementation of
// Store, backed by plain Go maps.
//
// Scaffold stub: all methods return errors.ErrNotImplemented.
type MemoryStore struct {
	mu      sync.RWMutex
	regular map[string][]byte
	intents map[string][]byte
}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// ApplyWriteBatch atomically applies wb to the database selected by kind.
//
// Not yet implemented in this scaffold.
func (s *MemoryStore) ApplyWriteBatch(ctx context.Context, kind DBKind, wb WriteBatch) error {
	return errors.ErrNotImplemented
}

// Get returns the value stored under key in the database selected by
// kind, and whether it was found.
//
// Not yet implemented in this scaffold.
func (s *MemoryStore) Get(ctx context.Context, kind DBKind, key []byte) ([]byte, bool, error) {
	return nil, false, errors.ErrNotImplemented
}

// NewIterator returns an Iterator over keys with the given prefix in the
// database selected by kind, in key order.
//
// Not yet implemented in this scaffold.
func (s *MemoryStore) NewIterator(ctx context.Context, kind DBKind, prefix []byte) (Iterator, error) {
	return nil, errors.ErrNotImplemented
}
