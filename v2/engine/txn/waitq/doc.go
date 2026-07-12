// Package waitq implements the wait-for graph and deadlock detection used
// by the distributed transaction manager: WaitQueue tracks which
// transactions are waiting on which blockers and releases waiters when a
// blocker resolves, while DeadlockDetector periodically scans the graph for
// cycles and aborts a deterministic victim from each. It is consumed by
// v2/engine/txn and v2/engine/txn/conflict when Config.UseWaitQueue is
// enabled.
// This is scaffold-only; behavior is unimplemented.
package waitq
