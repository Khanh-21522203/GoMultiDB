package conflict

import (
	"context"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/ids"
)

// TxnStatus represents the known status of a transaction.
type TxnStatus int

// Transaction status values as seen by the conflict resolver.
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

// IntentScanner scans the intents DB for a given set of keys and returns
// any conflicting intents. The returned slice may be empty.
type IntentScanner interface {
	ScanIntents(ctx context.Context, keys [][]byte) ([]Intent, error)
}

// StatusSource resolves a transaction's current status. Implementations
// may consult a local cache first and fall back to a coordinator RPC.
type StatusSource interface {
	GetStatus(ctx context.Context, txnID ids.TxnID) (StatusResult, error)
}

// AbortFn aborts a lower-priority conflicting transaction.
type AbortFn func(ctx context.Context, txnID ids.TxnID, reason string) error

// Config controls resolver behavior.
type Config struct {
	// UseWaitQueue, when true, enqueues the current txn in the wait queue
	// instead of aborting the conflicting txn when it is PENDING.
	// When false (default), the lower-priority txn is aborted immediately.
	UseWaitQueue bool
}

// Resolver resolves write-intent conflicts for a transaction.
//
// Scaffold stub: Check returns errors.ErrNotImplemented.
type Resolver struct {
	scanner   IntentScanner
	statusSrc StatusSource
	abortFn   AbortFn
	cfg       Config
}

// New creates a Resolver.
//
//   - scanner: scans the intents DB for the keys being accessed.
//   - statusSrc: resolves transaction status (possibly via coordinator RPC).
//   - abortFn: aborts a conflicting lower-priority transaction.
//   - cfg: resolver tuning options.
func New(scanner IntentScanner, statusSrc StatusSource, abortFn AbortFn, cfg Config) *Resolver {
	return &Resolver{}
}

// Check examines all keys for conflicting intents and applies the decision
// matrix described in the package doc.
//
//   - keys: byte-slice keys the caller wants to read or write.
//   - myTxnID: the transaction performing the check.
//   - myPriority: priority of the calling transaction (higher → more important).
//   - snapshotHT: the hybrid timestamp at which the caller is reading.
//
// Returns nil if there are no blocking conflicts. Returns
// ErrTxnRestartRequired if a committed intent is newer than snapshotHT.
// Returns ErrConflict if a lower-priority txn was aborted and the caller
// should retry after a short back-off.
//
// Not yet implemented in this scaffold.
func (r *Resolver) Check(
	ctx context.Context,
	keys [][]byte,
	myTxnID ids.TxnID,
	myPriority uint64,
	snapshotHT uint64,
) error {
	return errors.ErrNotImplemented
}
