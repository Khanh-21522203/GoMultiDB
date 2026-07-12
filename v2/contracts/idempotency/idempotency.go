package idempotency

import (
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/ids"
)

// Store detects and rejects duplicate requests within a scope. A request is
// identified by its RequestID; a caller-supplied fingerprint distinguishes
// a legitimate retry (same fingerprint) from a conflicting reuse of the
// same RequestID for different content (a different fingerprint).
type Store interface {
	// Seen reports whether id has already been recorded within scope, and
	// if so, the fingerprint it was recorded with.
	Seen(scope string, id ids.RequestID) (seen bool, fingerprint string, err error)
	// Mark records id within scope under the given fingerprint, expiring
	// after ttl.
	Mark(scope string, id ids.RequestID, fingerprint string, ttl time.Duration) error
}

// MemoryStore is an in-memory, non-durable reference implementation of
// Store, suitable for single-process testing.
//
// Scaffold stub: all methods return dberrors.ErrNotImplemented.
type MemoryStore struct{}

var _ Store = (*MemoryStore)(nil)

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// Seen reports whether id has already been recorded within scope.
//
// Not yet implemented in this scaffold.
func (m *MemoryStore) Seen(scope string, id ids.RequestID) (bool, string, error) {
	return false, "", dberrors.ErrNotImplemented
}

// Mark records id within scope under the given fingerprint, expiring after
// ttl.
//
// Not yet implemented in this scaffold.
func (m *MemoryStore) Mark(scope string, id ids.RequestID, fingerprint string, ttl time.Duration) error {
	return dberrors.ErrNotImplemented
}
