package waitq

import (
	"sync"
	"time"
)

// AbortFunc is called by the detector when it selects a victim.
// The implementation should abort the transaction with the given txnID.
type AbortFunc func(txnID [16]byte, reason string)

// PriorityFunc returns the priority of a transaction.
// Lower values are more likely to be selected as victims.
type PriorityFunc func(txnID [16]byte) uint64

// DeadlockDetector periodically scans the WaitQueue for cycles and aborts a
// deterministic victim from each detected cycle.
//
// Start() and Stop() control the background goroutine.
//
// Scaffold stub: all behavior-bearing methods panic.
type DeadlockDetector struct {
	queue       *WaitQueue
	interval    time.Duration
	abort       AbortFunc
	getPriority PriorityFunc
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewDetector creates a DeadlockDetector. It does not start until Start()
// is called.
//
//   - queue:       the wait-for graph to monitor.
//   - interval:    how often to scan (plan default: 1 s).
//   - abort:       called with the victim txnID and a reason string.
//   - getPriority: returns the priority for a txnID (0 = lowest/most expendable).
func NewDetector(queue *WaitQueue, interval time.Duration, abort AbortFunc, getPriority PriorityFunc) *DeadlockDetector {
	return &DeadlockDetector{}
}

// Start launches the background detection goroutine.
//
// Not yet implemented in this scaffold.
func (d *DeadlockDetector) Start() {
	panic("not implemented")
}

// Stop halts the background goroutine and waits for it to exit.
//
// Not yet implemented in this scaffold.
func (d *DeadlockDetector) Stop() {
	panic("not implemented")
}

// RunOnce executes one detection pass synchronously. Useful for tests.
//
// Not yet implemented in this scaffold.
func (d *DeadlockDetector) RunOnce() {
	panic("not implemented")
}
