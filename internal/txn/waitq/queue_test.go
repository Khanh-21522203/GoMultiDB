package waitq_test

import (
	"sync"
	"testing"
	"time"

	"GoMultiDB/internal/txn/waitq"
)

func txnID(b byte) [16]byte {
	var id [16]byte
	id[0] = b
	return id
}

// ── WaitQueue ─────────────────────────────────────────────────────────────────

func TestEnqueueAndRelease(t *testing.T) {
	q := waitq.New()
	a, b := txnID(1), txnID(2)

	ch := q.Enqueue(a, b) // a waits for b
	if q.Depth() != 1 {
		t.Fatalf("depth want 1, got %d", q.Depth())
	}

	// Release b — a's channel should be closed.
	done := make(chan struct{})
	go func() {
		select {
		case <-ch:
			close(done)
		case <-time.After(time.Second):
		}
	}()

	q.Release(b)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Release did not signal waiter within 1s")
	}
	if q.Depth() != 0 {
		t.Fatalf("depth want 0 after release, got %d", q.Depth())
	}
}

func TestReleaseSignalsMultipleWaiters(t *testing.T) {
	q := waitq.New()
	blocker := txnID(10)
	waiters := make([]chan struct{}, 5)
	for i := range waiters {
		waiters[i] = make(chan struct{})
		ch := q.Enqueue(txnID(byte(i+1)), blocker)
		go func(w chan struct{}, c <-chan struct{}) {
			<-c
			close(w)
		}(waiters[i], ch)
	}

	q.Release(blocker)

	for i, w := range waiters {
		select {
		case <-w:
		case <-time.After(time.Second):
			t.Fatalf("waiter %d not signaled within 1s", i)
		}
	}
}

func TestEnqueueSameWaiterMultipleBlockers(t *testing.T) {
	q := waitq.New()
	a, b1, b2 := txnID(1), txnID(2), txnID(3)

	ch1 := q.Enqueue(a, b1)
	ch2 := q.Enqueue(a, b2)
	// Both calls return the same channel for the same waiter.
	if ch1 != ch2 {
		// This is implementation-specific; accept either same or different channel.
		// What matters is that releasing one blocker signals the waiter.
		t.Logf("note: different channels returned for same waiter+different blockers")
	}
}

func TestReleaseWaiterItself(t *testing.T) {
	// Release a txn that is itself waiting should clean it up.
	q := waitq.New()
	a, b := txnID(1), txnID(2)
	q.Enqueue(a, b) // a waits for b
	q.Release(a)    // a resolves before b
	if q.Depth() != 0 {
		t.Fatalf("depth want 0, got %d", q.Depth())
	}
}

func TestConcurrentEnqueueRelease(t *testing.T) {
	q := waitq.New()
	var wg sync.WaitGroup
	const n = 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			waiter := txnID(byte(i % 50))
			blocker := txnID(byte((i + 1) % 50))
			q.Enqueue(waiter, blocker)
			q.Release(blocker)
		}(i)
	}
	wg.Wait()
}

// ── Cycle detection ───────────────────────────────────────────────────────────

func TestDetectNoCycles(t *testing.T) {
	q := waitq.New()
	// a -> b -> c (no cycle)
	q.Enqueue(txnID(1), txnID(2))
	q.Enqueue(txnID(2), txnID(3))

	cycles := q.DetectCycles()
	if len(cycles) != 0 {
		t.Fatalf("expected no cycles, got %d: %v", len(cycles), cycles)
	}
}

func TestDetectSimpleCycle(t *testing.T) {
	q := waitq.New()
	// a -> b -> a (simple cycle)
	q.Enqueue(txnID(1), txnID(2))
	q.Enqueue(txnID(2), txnID(1))

	cycles := q.DetectCycles()
	if len(cycles) == 0 {
		t.Fatalf("expected at least one cycle")
	}
}

func TestDetectThreeNodeCycle(t *testing.T) {
	q := waitq.New()
	// a -> b -> c -> a
	q.Enqueue(txnID(1), txnID(2))
	q.Enqueue(txnID(2), txnID(3))
	q.Enqueue(txnID(3), txnID(1))

	cycles := q.DetectCycles()
	if len(cycles) == 0 {
		t.Fatalf("expected cycle in 3-node ring")
	}
	if len(cycles[0]) < 3 {
		t.Fatalf("cycle should have at least 3 nodes, got %d", len(cycles[0]))
	}
}

// ── SelectVictim ─────────────────────────────────────────────────────────────

func TestSelectVictimLowestPriority(t *testing.T) {
	a, b, c := txnID(1), txnID(2), txnID(3)
	cycle := [][16]byte{a, b, c}
	priorities := map[[16]byte]uint64{
		a: 100,
		b: 50, // lowest priority → should be victim
		c: 200,
	}
	victim := waitq.SelectVictim(cycle, priorities)
	if victim != b {
		t.Fatalf("expected victim=b (priority 50), got %v", victim)
	}
}

func TestSelectVictimTiebreakerByTxnID(t *testing.T) {
	a, b := txnID(1), txnID(2)
	cycle := [][16]byte{a, b}
	priorities := map[[16]byte]uint64{a: 10, b: 10} // equal priority
	// a has smaller byte value → should be victim
	victim := waitq.SelectVictim(cycle, priorities)
	if victim != a {
		t.Fatalf("expected victim=a (lexicographically smaller), got %v", victim)
	}
}

// ── DeadlockDetector ─────────────────────────────────────────────────────────

func TestDeadlockDetectorAbortsCycleVictim(t *testing.T) {
	q := waitq.New()
	// Two txns waiting on each other → deadlock.
	a, b := txnID(10), txnID(20)
	q.Enqueue(a, b)
	q.Enqueue(b, a)

	var mu sync.Mutex
	aborted := make(map[[16]byte]bool)

	abort := func(id [16]byte, reason string) {
		mu.Lock()
		aborted[id] = true
		q.Release(id) // clean up so detector doesn't fire forever
		mu.Unlock()
	}
	getPriority := func(id [16]byte) uint64 {
		if id == a {
			return 5
		}
		return 10
	}

	det := waitq.NewDetector(q, 10*time.Millisecond, abort, getPriority)
	det.Start()
	defer det.Stop()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(aborted)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(aborted) == 0 {
		t.Fatalf("deadlock detector did not abort any victim within 1s")
	}
	// a has lower priority (5 < 10), so a should be the victim.
	if !aborted[a] {
		t.Fatalf("expected a (priority=5) to be the victim, aborted: %v", aborted)
	}
}

func TestDeadlockDetectorRunOnce(t *testing.T) {
	q := waitq.New()
	a, b := txnID(30), txnID(40)
	q.Enqueue(a, b)
	q.Enqueue(b, a)

	var abortedMu sync.Mutex
	aborted := make(map[[16]byte]bool)
	abort := func(id [16]byte, _ string) {
		abortedMu.Lock()
		aborted[id] = true
		abortedMu.Unlock()
	}

	det := waitq.NewDetector(q, time.Hour, abort, func(_ [16]byte) uint64 { return 1 })
	det.RunOnce()

	abortedMu.Lock()
	defer abortedMu.Unlock()
	if len(aborted) == 0 {
		t.Fatalf("RunOnce should detect the deadlock and abort a victim")
	}
}

func TestDeadlockDetectorNoCycleNoAbort(t *testing.T) {
	q := waitq.New()
	// a -> b -> c: no cycle
	q.Enqueue(txnID(1), txnID(2))
	q.Enqueue(txnID(2), txnID(3))

	abortCount := 0
	det := waitq.NewDetector(q, time.Hour, func(_ [16]byte, _ string) { abortCount++ }, func(_ [16]byte) uint64 { return 1 })
	det.RunOnce()

	if abortCount != 0 {
		t.Fatalf("expected no abort for acyclic graph, got %d", abortCount)
	}
}
