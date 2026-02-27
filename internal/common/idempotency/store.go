package idempotency

import (
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
)

type Store interface {
	Seen(scope string, id ids.RequestID) (seen bool, fingerprint string, err error)
	Mark(scope string, id ids.RequestID, fingerprint string, ttl time.Duration) error
}

type memoryEntry struct {
	fingerprint string
	expiresAt   time.Time
}

type MemoryStore struct {
	mu    sync.Mutex
	items map[string]memoryEntry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{items: make(map[string]memoryEntry)}
}

func key(scope string, id ids.RequestID) string {
	return scope + ":" + string(id)
}

func (m *MemoryStore) Seen(scope string, id ids.RequestID) (bool, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := key(scope, id)
	it, ok := m.items[k]
	if !ok {
		return false, "", nil
	}
	if time.Now().After(it.expiresAt) {
		delete(m.items, k)
		return false, "", nil
	}
	return true, it.fingerprint, nil
}

func (m *MemoryStore) Mark(scope string, id ids.RequestID, fingerprint string, ttl time.Duration) error {
	if ttl <= 0 {
		return dberrors.New(dberrors.ErrInvalidArgument, "idempotency ttl must be > 0", false, nil)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := key(scope, id)
	existing, ok := m.items[k]
	if ok && time.Now().Before(existing.expiresAt) && existing.fingerprint != fingerprint {
		return dberrors.New(dberrors.ErrIdempotencyConflict, "idempotency fingerprint mismatch", false, nil)
	}

	m.items[k] = memoryEntry{
		fingerprint: fingerprint,
		expiresAt:   time.Now().Add(ttl),
	}
	return nil
}
