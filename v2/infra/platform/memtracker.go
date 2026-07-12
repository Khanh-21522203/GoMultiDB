package platform

import (
	"sync"
	"sync/atomic"

	errors "GoMultiDB/v2/contracts/errors"
)

// MemTracker tracks memory consumption for a subsystem and enforces a hard
// limit. Child trackers can be registered for per-subsystem accounting;
// consumption is summed at the root level.
//
// Scaffold stub: all behavior-bearing methods return errors.ErrNotImplemented
// or panic.
type MemTracker struct {
	name           string
	hardLimitBytes int64 // 0 means unlimited
	currentUsage   atomic.Int64

	mu       sync.RWMutex
	children map[string]*MemTracker
	parent   *MemTracker
}

// NewMemTracker creates a root MemTracker with the given hard limit. If
// hardLimitBytes is 0, no limit is enforced.
func NewMemTracker(hardLimitBytes int64) *MemTracker {
	return &MemTracker{}
}

// NewChild creates and registers a named child tracker. The child inherits
// the root's hard limit and contributes to root-level accounting.
func (m *MemTracker) NewChild(name string) *MemTracker {
	return &MemTracker{}
}

// Consume adds bytes to the tracker's usage, propagating to the parent.
// Returns ErrOOMKill (via a DBError) if the root hard limit would be
// exceeded.
//
// Not yet implemented in this scaffold.
func (m *MemTracker) Consume(bytes int64) error {
	return errors.ErrNotImplemented
}

// Release subtracts bytes from the tracker's usage, propagating to the
// parent.
//
// Not yet implemented in this scaffold.
func (m *MemTracker) Release(bytes int64) {
	panic("not implemented")
}

// CurrentUsage returns the current memory usage in bytes for this tracker.
//
// Not yet implemented in this scaffold.
func (m *MemTracker) CurrentUsage() int64 {
	panic("not implemented")
}

// Name returns the tracker's name.
//
// Not yet implemented in this scaffold.
func (m *MemTracker) Name() string {
	panic("not implemented")
}
