// Package waitq implements the wait-for graph and deadlock detection used
// by the distributed transaction manager.
//
// Design:
//   - WaitQueue tracks which transactions are waiting on which blockers.
//   - When a blocker resolves (commit or abort), Release unblocks all waiters.
//   - DeadlockDetector runs periodic DFS on the graph to find cycles and
//     aborts a deterministic victim (lowest priority, lowest TxnID tiebreak).
package waitq

import (
	"bytes"
	"sort"
	"sync"
)

// WaitQueue tracks the wait-for graph among pending transactions.
// Enqueue adds a waiter→blocker edge; Release removes a resolved transaction.
type WaitQueue struct {
	mu    sync.Mutex
	graph map[[16]byte]map[[16]byte]struct{} // waiter -> set of blockers
	notif map[[16]byte]chan struct{}           // waiter -> signal channel (closed on any release)
}

// New returns an empty WaitQueue.
func New() *WaitQueue {
	return &WaitQueue{
		graph: make(map[[16]byte]map[[16]byte]struct{}),
		notif: make(map[[16]byte]chan struct{}),
	}
}

// Enqueue records that waiter is blocked by blocker and returns a channel that
// will be closed when Release(blocker) is called. Callers should select on the
// returned channel to resume after the blocker resolves.
//
// Multiple calls with the same waiter accumulate blockers; the returned channel
// is shared — it is closed when ANY blocker for waiter is released.
func (q *WaitQueue) Enqueue(waiter, blocker [16]byte) <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.graph[waiter] == nil {
		q.graph[waiter] = make(map[[16]byte]struct{})
	}
	q.graph[waiter][blocker] = struct{}{}

	if q.notif[waiter] == nil {
		q.notif[waiter] = make(chan struct{})
	}
	return q.notif[waiter]
}

// Release removes txnID from the graph (as both a blocker and as a waiter)
// and signals all transactions that were waiting on txnID by closing their
// notification channels. Callers that receive the signal should re-check for
// conflicts and re-enqueue if the new state still blocks them.
func (q *WaitQueue) Release(txnID [16]byte) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Signal and unblock all waiters that listed txnID as a blocker.
	for waiter, blockers := range q.graph {
		if _, blocked := blockers[txnID]; !blocked {
			continue
		}
		delete(blockers, txnID)
		if ch, ok := q.notif[waiter]; ok {
			close(ch)
			delete(q.notif, waiter)
		}
		if len(blockers) == 0 {
			delete(q.graph, waiter)
		}
	}

	// Remove txnID as a waiter (it resolved, so it no longer waits for anything).
	delete(q.graph, txnID)
	if ch, ok := q.notif[txnID]; ok {
		close(ch)
		delete(q.notif, txnID)
	}
}

// Depth returns the current number of waiting transactions.
func (q *WaitQueue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.graph)
}

// Graph returns a stable snapshot of the wait-for graph for cycle detection.
// The returned map is a deep copy; it is safe to read after releasing the lock.
func (q *WaitQueue) Graph() map[[16]byte][][16]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[[16]byte][][16]byte, len(q.graph))
	for w, blockers := range q.graph {
		bs := make([][16]byte, 0, len(blockers))
		for b := range blockers {
			bs = append(bs, b)
		}
		// Stable order for deterministic tests.
		sort.Slice(bs, func(i, j int) bool {
			return bytes.Compare(bs[i][:], bs[j][:]) < 0
		})
		out[w] = bs
	}
	return out
}

// DetectCycles finds all simple cycles in the current wait-for graph using
// iterative DFS with three-color marking (white / gray / black).
// Each returned cycle is a minimal list of TxnIDs forming the loop.
func (q *WaitQueue) DetectCycles() [][][16]byte {
	return detectCycles(q.Graph())
}

// detectCycles runs DFS on graph and returns all detected cycles.
func detectCycles(graph map[[16]byte][][16]byte) [][][16]byte {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[[16]byte]int, len(graph))
	path := make([][16]byte, 0, len(graph))
	var cycles [][][16]byte

	// Deterministic node iteration order.
	nodes := make([][16]byte, 0, len(graph))
	for n := range graph {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i][:], nodes[j][:]) < 0
	})

	var dfs func(node [16]byte)
	dfs = func(node [16]byte) {
		color[node] = gray
		path = append(path, node)

		for _, neighbor := range graph[node] {
			switch color[neighbor] {
			case white:
				dfs(neighbor)
			case gray:
				// Back edge — found a cycle.
				if cycle := extractCycle(path, neighbor); len(cycle) > 0 {
					cycles = append(cycles, cycle)
				}
			}
			// black: already fully explored, skip
		}

		path = path[:len(path)-1]
		color[node] = black
	}

	for _, n := range nodes {
		if color[n] == white {
			dfs(n)
		}
	}
	return cycles
}

// extractCycle returns the portion of path starting at the first occurrence of
// start, forming the cycle back to start.
func extractCycle(path [][16]byte, start [16]byte) [][16]byte {
	for i, n := range path {
		if n == start {
			cycle := make([][16]byte, len(path)-i)
			copy(cycle, path[i:])
			return cycle
		}
	}
	return nil
}

// SelectVictim picks the transaction to abort from a deadlock cycle.
//
// Selection rules (from plan):
//  1. Lowest priority value (lower number = more expendable).
//  2. Lexicographically smallest TxnID as tiebreaker for determinism.
func SelectVictim(cycle [][16]byte, priorities map[[16]byte]uint64) [16]byte {
	if len(cycle) == 0 {
		panic("waitq: SelectVictim called with empty cycle")
	}
	victim := cycle[0]
	for _, txn := range cycle[1:] {
		vp := priorities[victim]
		tp := priorities[txn]
		if tp < vp || (tp == vp && bytes.Compare(txn[:], victim[:]) < 0) {
			victim = txn
		}
	}
	return victim
}
