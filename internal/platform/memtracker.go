// Package platform provides low-level platform services: filesystem layout,
// memory tracking, and related utilities.
package platform

import (
	"fmt"
	"sync"
	"sync/atomic"

	dberrors "GoMultiDB/internal/common/errors"
)

// MemTracker tracks memory consumption for a subsystem and enforces a hard limit.
// Child trackers can be registered for per-subsystem accounting; consumption is
// summed at the root level.
type MemTracker struct {
	name           string
	hardLimitBytes int64 // 0 means unlimited
	currentUsage   atomic.Int64

	mu       sync.RWMutex
	children map[string]*MemTracker
	parent   *MemTracker
}

// NewMemTracker creates a root MemTracker with the given hard limit.
// If hardLimitBytes is 0, no limit is enforced.
func NewMemTracker(hardLimitBytes int64) *MemTracker {
	return &MemTracker{
		name:           "root",
		hardLimitBytes: hardLimitBytes,
		children:       make(map[string]*MemTracker),
	}
}

// NewChild creates and registers a named child tracker. The child inherits
// the root's hard limit and contributes to root-level accounting.
func (m *MemTracker) NewChild(name string) *MemTracker {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.children[name]; ok {
		return c
	}
	c := &MemTracker{
		name:           name,
		hardLimitBytes: m.hardLimitBytes,
		children:       make(map[string]*MemTracker),
		parent:         m,
	}
	m.children[name] = c
	return c
}

// Consume adds bytes to the tracker's usage, propagating to the parent.
// Returns ErrOOMKill if the root hard limit would be exceeded.
func (m *MemTracker) Consume(bytes int64) error {
	root := m.root()
	if root.hardLimitBytes > 0 {
		projected := root.currentUsage.Load() + bytes
		if projected > root.hardLimitBytes {
			return dberrors.New(dberrors.ErrOOMKill,
				fmt.Sprintf("memory hard limit exceeded: limit=%d projected=%d",
					root.hardLimitBytes, projected),
				false, nil)
		}
	}
	m.currentUsage.Add(bytes)
	if m.parent != nil {
		m.parent.currentUsage.Add(bytes)
	}
	return nil
}

// Release subtracts bytes from the tracker's usage, propagating to the parent.
func (m *MemTracker) Release(bytes int64) {
	m.currentUsage.Add(-bytes)
	if m.parent != nil {
		m.parent.currentUsage.Add(-bytes)
	}
}

// CurrentUsage returns the current memory usage in bytes for this tracker.
func (m *MemTracker) CurrentUsage() int64 {
	return m.currentUsage.Load()
}

// Name returns the tracker's name.
func (m *MemTracker) Name() string { return m.name }

// root traverses to the root tracker.
func (m *MemTracker) root() *MemTracker {
	cur := m
	for cur.parent != nil {
		cur = cur.parent
	}
	return cur
}
