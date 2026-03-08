// Package conflict implements intent conflict resolution for the distributed
// transaction manager.
//
// Design (from docs/plans/plan-distributed-transaction-manager.md §Conflict Resolver):
//
//   - Scan the intents DB for each key the current transaction wants to touch.
//   - For each conflicting intent look up the owning transaction's status via
//     a StatusSource (coordinator RPC in production, in-memory stub in tests).
//   - Decision matrix:
//       ABORTED   → safe to ignore (stale intent, will be cleaned up)
//       COMMITTED → if commit_ht > snapshot_ht return ErrTxnRestartRequired
//       PENDING   → priority-based: abort lower-priority txn via AbortFn,
//                   or wait in queue if wait-queues are enabled.
package conflict

import (
	"context"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
)

// TxnStatus represents the known status of a transaction.
type TxnStatus int

const (
	// StatusUnknown means the status is not cached locally.
	StatusUnknown TxnStatus = iota
	// StatusPending — transaction is still in progress.
	StatusPending
	// StatusCommitted — transaction committed at CommitHT.
	StatusCommitted
	// StatusAborted — transaction was aborted.
	StatusAborted
)

// StatusResult is returned by StatusSource for a given transaction.
type StatusResult struct {
	Status   TxnStatus
	CommitHT uint64 // only meaningful when Status == StatusCommitted
	Priority uint64 // only meaningful when Status == StatusPending
}

// Intent represents a single intent record found in the intents DB.
type Intent struct {
	TxnID    ids.TxnID
	Priority uint64 // writer's declared priority (may be 0 if unknown)
}

// IntentScanner scans the intents DB for a given set of keys and returns any
// conflicting intents.  The returned slice may be empty.
type IntentScanner interface {
	ScanIntents(ctx context.Context, keys [][]byte) ([]Intent, error)
}

// StatusSource resolves a transaction's current status.  Implementations may
// consult a local cache first and fall back to a coordinator RPC.
type StatusSource interface {
	GetStatus(ctx context.Context, txnID ids.TxnID) (StatusResult, error)
}

// AbortFn aborts a lower-priority conflicting transaction.
type AbortFn func(ctx context.Context, txnID ids.TxnID, reason string) error

// Config controls resolver behaviour.
type Config struct {
	// UseWaitQueue, when true, enqueues the current txn in the wait queue
	// instead of aborting the conflicting txn when it is PENDING.
	// When false (default), the lower-priority txn is aborted immediately.
	UseWaitQueue bool
}

// Resolver resolves write-intent conflicts for a transaction.
type Resolver struct {
	scanner    IntentScanner
	statusSrc  StatusSource
	abortFn    AbortFn
	cfg        Config
}

// New creates a Resolver.
//
//   - scanner: scans the intents DB for the keys being accessed.
//   - statusSrc: resolves transaction status (possibly via coordinator RPC).
//   - abortFn: aborts a conflicting lower-priority transaction.
//   - cfg: resolver tuning options.
func New(scanner IntentScanner, statusSrc StatusSource, abortFn AbortFn, cfg Config) *Resolver {
	return &Resolver{
		scanner:   scanner,
		statusSrc: statusSrc,
		abortFn:   abortFn,
		cfg:       cfg,
	}
}

// Check examines all keys for conflicting intents and applies the decision
// matrix described in the package doc.
//
//   - keys: byte-slice keys the caller wants to read or write.
//   - myTxnID: the transaction performing the check.
//   - myPriority: priority of the calling transaction (higher → more important).
//   - snapshotHT: the hybrid timestamp at which the caller is reading.
//
// Returns nil if there are no blocking conflicts.
// Returns ErrTxnRestartRequired if a committed intent is newer than snapshotHT.
// Returns ErrConflict if a lower-priority txn was aborted and the caller should
// retry after a short back-off (the conflicting intent will be cleaned up).
func (r *Resolver) Check(
	ctx context.Context,
	keys [][]byte,
	myTxnID ids.TxnID,
	myPriority uint64,
	snapshotHT uint64,
) error {
	intents, err := r.scanner.ScanIntents(ctx, keys)
	if err != nil {
		return dberrors.New(dberrors.ErrInternal, "scan intents failed", true, err)
	}

	for _, intent := range intents {
		// Never conflict with ourselves.
		if intent.TxnID == myTxnID {
			continue
		}

		sr, err := r.statusSrc.GetStatus(ctx, intent.TxnID)
		if err != nil {
			// Treat lookup failure as a retryable internal error.
			return dberrors.New(dberrors.ErrRetryableUnavailable, "status lookup failed", true, err)
		}

		switch sr.Status {
		case StatusAborted:
			// Stale intent — ignore; the intent will be cleaned up asynchronously.
			continue

		case StatusCommitted:
			// If the committed write is newer than our snapshot we must restart
			// at a higher timestamp so we see the correct committed value.
			if sr.CommitHT > snapshotHT {
				return dberrors.New(dberrors.ErrTxnRestartRequired,
					"committed intent newer than snapshot", false, nil)
			}
			// commit_ht <= snapshot_ht: our snapshot already captures this value, no conflict.

		case StatusPending, StatusUnknown:
			// Conflict with an in-progress transaction.
			// Decide: abort the lower-priority txn, or wait.
			conflictPriority := sr.Priority
			if intent.Priority > 0 {
				// Use the intent's cached priority if the status source didn't
				// populate it (e.g., cache miss returned StatusUnknown).
				conflictPriority = intent.Priority
			}

			if r.cfg.UseWaitQueue {
				// Caller should enqueue itself in the wait queue and block.
				// We signal this via ErrRetryableUnavailable so the caller
				// knows to back off without a hard abort.
				return dberrors.New(dberrors.ErrRetryableUnavailable,
					"pending conflict; caller should wait in queue", true, nil)
			}

			// Priority-based preemption: abort the lower-priority transaction.
			if conflictPriority <= myPriority {
				// Abort the conflicting (lower or equal priority) txn.
				if err := r.abortFn(ctx, intent.TxnID, "aborted by higher-priority transaction"); err != nil {
					return dberrors.New(dberrors.ErrRetryableUnavailable, "abort conflicting txn failed", true, err)
				}
				// Caller should retry after the abort propagates.
				return dberrors.New(dberrors.ErrConflict, "conflicting intent aborted; retry", true, nil)
			}
			// myPriority < conflictPriority: we are lower priority; caller must abort itself.
			return dberrors.New(dberrors.ErrConflict, "higher-priority transaction holds conflicting intent", false, nil)
		}
	}

	return nil
}
