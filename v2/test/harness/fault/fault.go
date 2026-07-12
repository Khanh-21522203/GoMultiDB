package fault

import (
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// FaultType identifies the kind of fault to inject.
type FaultType string

// FaultType values selecting which kind of fault InjectFault activates.
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
//
// Scaffold stub: all behavior-bearing methods return errors.ErrNotImplemented
// or panic.
type FaultInjector struct {
	mu      sync.RWMutex
	faults  map[string]FaultAction // keyed by FaultID
	counter int64
}

// NewFaultInjector creates an empty FaultInjector.
func NewFaultInjector() *FaultInjector {
	return &FaultInjector{}
}

// InjectFault activates the given fault. If FaultID is empty, a unique ID is
// generated and set on the returned action.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) InjectFault(action FaultAction) (FaultAction, error) {
	return FaultAction{}, errors.ErrNotImplemented
}

// HealFault removes the fault with the given ID. Returns an error if not found.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) HealFault(faultID string) error {
	return errors.ErrNotImplemented
}

// HealAll removes all active faults.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) HealAll() {
	panic("not implemented")
}

// ActiveFaults returns a snapshot of all currently active faults.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) ActiveFaults() []FaultAction {
	panic("not implemented")
}

// IsPartitioned returns true if there is an active partition fault between
// from and to in either direction.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) IsPartitioned(from, to string) bool {
	panic("not implemented")
}

// DelayFor returns the maximum delay to apply for an RPC from → to, or 0 if
// none.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) DelayFor(from, to string) time.Duration {
	panic("not implemented")
}

// IsKilled returns true if there is an active kill fault for the given node.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) IsKilled(nodeID string) bool {
	panic("not implemented")
}

// IsDiskFull returns true if there is an active diskfull fault for the given
// node.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) IsDiskFull(nodeID string) bool {
	panic("not implemented")
}

// ClockSkewFor returns the active clock skew offset for the given node, or 0.
//
// Not yet implemented in this scaffold.
func (fi *FaultInjector) ClockSkewFor(nodeID string) time.Duration {
	panic("not implemented")
}
