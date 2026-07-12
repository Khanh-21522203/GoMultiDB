package txn

import (
	"context"
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/ids"
)

// State is a transaction's lifecycle stage.
type State int

// Transaction lifecycle states.
const (
	Created State = iota
	Pending
	Committing
	Committed
	Aborting
	Aborted
)

// IntentApplier resolves the intents a transaction staged across tablets
// once the transaction commits or aborts.
type IntentApplier interface {
	// ApplyIntents moves up to limit of txnID's intents into committed
	// storage at commitHT. done reports whether all intents were applied.
	ApplyIntents(ctx context.Context, txnID [16]byte, commitHT uint64, limit int) (done bool, err error)
	// RemoveIntents discards up to limit of txnID's intents without
	// applying them. done reports whether all intents were removed.
	RemoveIntents(ctx context.Context, txnID [16]byte, limit int) (done bool, err error)
}

// IsolationLevel describes the transaction's consistency guarantee.
type IsolationLevel int

const (
	// SerializableIsolation is the strongest level — full serializability.
	SerializableIsolation IsolationLevel = iota
	// SnapshotIsolation (Repeatable Read) — reads from a stable snapshot.
	SnapshotIsolation
)

// Record is the coordinator-side state for a single distributed
// transaction.
type Record struct {
	TxnID    ids.TxnID
	State    State
	CommitHT uint64

	// Priority is used by the conflict resolver and deadlock detector.
	// Higher values → more important; lower values → preferred victim.
	Priority uint64

	// StartHT is the hybrid timestamp when Begin() was called.
	StartHT uint64

	// Isolation level requested by the client.
	Isolation IsolationLevel

	// StatusTablet is the tablet ID that durably stores this txn's status.
	// Empty in the current in-memory implementation.
	StatusTablet ids.TabletID

	// InvolvedTablets accumulates the set of tablets that have received
	// intents from this transaction. Used by Commit to dispatch apply
	// tasks.
	InvolvedTablets map[ids.TabletID]struct{}

	LastHeartbeat time.Time
	RequestID     ids.RequestID
}

// Config tunes a Manager's timeout and batching behavior.
type Config struct {
	TxnTimeout      time.Duration
	NowFn           func() time.Time
	ApplyBatchLimit int
}

// Manager is the coordinator-side distributed transaction manager.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented or panic.
type Manager struct {
	mu      sync.Mutex
	records map[ids.TxnID]Record
	cfg     Config
	applier IntentApplier
}

// NewManager returns a Manager using cfg and applier to resolve intents on
// commit/abort. Zero-valued fields in cfg are defaulted (120s timeout, a
// wall-clock NowFn, and a 1000-item apply batch limit).
func NewManager(cfg Config, applier IntentApplier) *Manager {
	return &Manager{}
}

// BeginOptions carries optional metadata for a new transaction.
type BeginOptions struct {
	Priority     uint64
	StartHT      uint64
	Isolation    IsolationLevel
	StatusTablet ids.TabletID
}

// Begin registers a new transaction under txnID, or, if txnID already
// exists with the same reqID, succeeds idempotently. Returns
// ErrIdempotencyConflict if txnID exists with a different reqID.
//
// Not yet implemented in this scaffold.
func (m *Manager) Begin(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID, opts BeginOptions) error {
	return errors.ErrNotImplemented
}

// RegisterTablet adds tabletID to the set of tablets involved in txnID.
// Idempotent — registering the same tablet multiple times is safe.
//
// Not yet implemented in this scaffold.
func (m *Manager) RegisterTablet(ctx context.Context, txnID ids.TxnID, tabletID ids.TabletID) error {
	return errors.ErrNotImplemented
}

// Heartbeat refreshes txnID's last-heartbeat time, postponing expiry.
//
// Not yet implemented in this scaffold.
func (m *Manager) Heartbeat(ctx context.Context, txnID ids.TxnID) error {
	return errors.ErrNotImplemented
}

// Commit drives txnID to Committed at commitHT, applying its staged
// intents via the configured IntentApplier. Returns the commit HT.
//
// Not yet implemented in this scaffold.
func (m *Manager) Commit(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID, commitHT uint64) (uint64, error) {
	return 0, errors.ErrNotImplemented
}

// Abort drives txnID to Aborted, removing its staged intents via the
// configured IntentApplier.
//
// Not yet implemented in this scaffold.
func (m *Manager) Abort(ctx context.Context, txnID ids.TxnID, reqID ids.RequestID) error {
	return errors.ErrNotImplemented
}

// ExpireStale aborts every non-terminal transaction whose last heartbeat is
// older than Config.TxnTimeout, returning the number expired.
//
// Not yet implemented in this scaffold.
func (m *Manager) ExpireStale() int {
	panic("not implemented")
}

// Get returns the Record for txnID, and whether it exists.
//
// Not yet implemented in this scaffold.
func (m *Manager) Get(txnID ids.TxnID) (Record, bool) {
	panic("not implemented")
}
