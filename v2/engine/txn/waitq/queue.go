package waitq

import "sync"

// WaitQueue tracks the wait-for graph among pending transactions.
// Enqueue adds a waiter→blocker edge; Release removes a resolved
// transaction.
//
// Scaffold stub: all behavior-bearing methods return zero values or panic.
type WaitQueue struct {
	mu    sync.Mutex
	graph map[[16]byte]map[[16]byte]struct{} // waiter -> set of blockers
	notif map[[16]byte]chan struct{}         // waiter -> signal channel (closed on any release)
}

// New returns an empty WaitQueue.
func New() *WaitQueue {
	return &WaitQueue{}
}

// Enqueue records that waiter is blocked by blocker and returns a channel
// that will be closed when Release(blocker) is called. Multiple calls with
// the same waiter accumulate blockers; the returned channel is shared — it
// is closed when ANY blocker for waiter is released.
//
// Not yet implemented in this scaffold.
func (q *WaitQueue) Enqueue(waiter, blocker [16]byte) <-chan struct{} {
	panic("not implemented")
}

// Release removes txnID from the graph (as both a blocker and as a waiter)
// and signals all transactions that were waiting on txnID by closing their
// notification channels.
//
// Not yet implemented in this scaffold.
func (q *WaitQueue) Release(txnID [16]byte) {
	panic("not implemented")
}

// Depth returns the current number of waiting transactions.
//
// Not yet implemented in this scaffold.
func (q *WaitQueue) Depth() int {
	panic("not implemented")
}

// Graph returns a stable snapshot of the wait-for graph for cycle
// detection. The returned map is a deep copy; it is safe to read after the
// call returns.
//
// Not yet implemented in this scaffold.
func (q *WaitQueue) Graph() map[[16]byte][][16]byte {
	panic("not implemented")
}

// DetectCycles finds all simple cycles in the current wait-for graph. Each
// returned cycle is a minimal list of TxnIDs forming the loop.
//
// Not yet implemented in this scaffold.
func (q *WaitQueue) DetectCycles() [][][16]byte {
	panic("not implemented")
}

// SelectVictim picks the transaction to abort from a deadlock cycle.
//
// Selection rules:
//  1. Lowest priority value (lower number = more expendable).
//  2. Lexicographically smallest TxnID as tiebreaker for determinism.
//
// Not yet implemented in this scaffold.
func SelectVictim(cycle [][16]byte, priorities map[[16]byte]uint64) [16]byte {
	panic("not implemented")
}
