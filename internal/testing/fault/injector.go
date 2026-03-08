// Package fault provides fault injection primitives for distributed system testing.
package fault

import (
	"fmt"
	"sync"
	"time"
)

// FaultType identifies the kind of fault to inject.
type FaultType string

const (
	// FaultPartition drops all RPC calls between FromNode and ToNode (bidirectional).
	FaultPartition FaultType = "partition"
	// FaultKill requests orderly shutdown of the target node.
	FaultKill FaultType = "kill"
	// FaultDelay injects artificial latency (Value as time.Duration) into RPC sends.
	FaultDelay FaultType = "delay"
	// FaultDiskFull makes FS writes on the target node return errors.
	FaultDiskFull FaultType = "diskfull"
	// FaultClockSkew offsets the hybrid clock on the target node by Value duration.
	FaultClockSkew FaultType = "clockskew"
)

// FaultAction describes a single fault to inject.
type FaultAction struct {
	// FaultID uniquely identifies this fault for later healing. Auto-assigned if empty.
	FaultID  string
	Type     FaultType
	FromNode string // used by: partition, delay
	ToNode   string // used by: partition, kill, diskfull, clockskew
	// Value carries numeric parameters:
	// FaultDelay — duration in ns (cast from time.Duration)
	// FaultClockSkew — offset in ns (cast from time.Duration)
	Value int64
}

// DelayDuration returns the delay value as time.Duration.
func (f FaultAction) DelayDuration() time.Duration { return time.Duration(f.Value) }

// ClockSkewDuration returns the clock skew value as time.Duration.
func (f FaultAction) ClockSkewDuration() time.Duration { return time.Duration(f.Value) }

// FaultInjector maintains the set of currently active faults and provides
// helpers to query them from interceptors in the RPC transport.
type FaultInjector struct {
	mu      sync.RWMutex
	faults  map[string]FaultAction // keyed by FaultID
	counter int64
}

// NewFaultInjector creates an empty FaultInjector.
func NewFaultInjector() *FaultInjector {
	return &FaultInjector{faults: make(map[string]FaultAction)}
}

// InjectFault activates the given fault. If FaultID is empty, a unique ID is
// generated and set on the returned action.
func (fi *FaultInjector) InjectFault(action FaultAction) (FaultAction, error) {
	if action.Type == "" {
		return FaultAction{}, fmt.Errorf("fault type is required")
	}
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if action.FaultID == "" {
		fi.counter++
		action.FaultID = fmt.Sprintf("fault-%d", fi.counter)
	}
	fi.faults[action.FaultID] = action
	return action, nil
}

// HealFault removes the fault with the given ID. Returns an error if not found.
func (fi *FaultInjector) HealFault(faultID string) error {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if _, ok := fi.faults[faultID]; !ok {
		return fmt.Errorf("fault %q not found", faultID)
	}
	delete(fi.faults, faultID)
	return nil
}

// HealAll removes all active faults.
func (fi *FaultInjector) HealAll() {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.faults = make(map[string]FaultAction)
}

// ActiveFaults returns a snapshot of all currently active faults.
func (fi *FaultInjector) ActiveFaults() []FaultAction {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	out := make([]FaultAction, 0, len(fi.faults))
	for _, f := range fi.faults {
		out = append(out, f)
	}
	return out
}

// IsPartitioned returns true if there is an active partition fault between from and to
// in either direction.
func (fi *FaultInjector) IsPartitioned(from, to string) bool {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	for _, f := range fi.faults {
		if f.Type != FaultPartition {
			continue
		}
		if (f.FromNode == from && f.ToNode == to) || (f.FromNode == to && f.ToNode == from) {
			return true
		}
	}
	return false
}

// DelayFor returns the maximum delay to apply for an RPC from → to, or 0 if none.
func (fi *FaultInjector) DelayFor(from, to string) time.Duration {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	var max time.Duration
	for _, f := range fi.faults {
		if f.Type != FaultDelay {
			continue
		}
		if f.FromNode == from && f.ToNode == to {
			if d := f.DelayDuration(); d > max {
				max = d
			}
		}
	}
	return max
}

// IsKilled returns true if there is an active kill fault for the given node.
func (fi *FaultInjector) IsKilled(nodeID string) bool {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	for _, f := range fi.faults {
		if f.Type == FaultKill && f.ToNode == nodeID {
			return true
		}
	}
	return false
}

// IsDiskFull returns true if there is an active diskfull fault for the given node.
func (fi *FaultInjector) IsDiskFull(nodeID string) bool {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	for _, f := range fi.faults {
		if f.Type == FaultDiskFull && f.ToNode == nodeID {
			return true
		}
	}
	return false
}

// ClockSkewFor returns the active clock skew offset for the given node, or 0.
func (fi *FaultInjector) ClockSkewFor(nodeID string) time.Duration {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	for _, f := range fi.faults {
		if f.Type == FaultClockSkew && f.ToNode == nodeID {
			return f.ClockSkewDuration()
		}
	}
	return 0
}
