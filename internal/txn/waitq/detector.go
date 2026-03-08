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
type DeadlockDetector struct {
	queue       *WaitQueue
	interval    time.Duration
	abort       AbortFunc
	getPriority PriorityFunc
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewDetector creates a DeadlockDetector. It does not start until Start() is called.
//
//   - queue:       the wait-for graph to monitor.
//   - interval:    how often to scan (plan default: 1 s).
//   - abort:       called with the victim txnID and a reason string.
//   - getPriority: returns the priority for a txnID (0 = lowest/most expendable).
func NewDetector(queue *WaitQueue, interval time.Duration, abort AbortFunc, getPriority PriorityFunc) *DeadlockDetector {
	if interval <= 0 {
		interval = time.Second
	}
	return &DeadlockDetector{
		queue:       queue,
		interval:    interval,
		abort:       abort,
		getPriority: getPriority,
		stopCh:      make(chan struct{}),
	}
}

// Start launches the background detection goroutine.
func (d *DeadlockDetector) Start() {
	d.wg.Add(1)
	go d.run()
}

// Stop halts the background goroutine and waits for it to exit.
func (d *DeadlockDetector) Stop() {
	close(d.stopCh)
	d.wg.Wait()
}

// RunOnce executes one detection pass synchronously. Useful for tests.
func (d *DeadlockDetector) RunOnce() {
	d.detect()
}

func (d *DeadlockDetector) run() {
	defer d.wg.Done()
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.detect()
		case <-d.stopCh:
			return
		}
	}
}

func (d *DeadlockDetector) detect() {
	cycles := d.queue.DetectCycles()
	for _, cycle := range cycles {
		priorities := make(map[[16]byte]uint64, len(cycle))
		for _, txn := range cycle {
			priorities[txn] = d.getPriority(txn)
		}
		victim := SelectVictim(cycle, priorities)
		d.abort(victim, "deadlock detected")
	}
}
