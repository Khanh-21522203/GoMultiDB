package docdb

import (
	"context"
	"sync/atomic"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/types"
	"GoMultiDB/v2/infra/storage/rocks"
)

// MutationOp identifies the kind of change a KVMutation applies.
type MutationOp string

// MutationSet is the only supported mutation kind: an unconditional
// key-value set.
const (
	MutationSet MutationOp = "set"
)

// KVMutation is a single key-value change within a DocWriteBatch.
type KVMutation struct {
	Key     []byte
	Value   []byte
	Op      MutationOp
	WriteHT uint64
}

// DocWriteBatch is an ordered set of mutations applied together.
type DocWriteBatch struct {
	Mutations []KVMutation
}

// TxnMeta carries the identifying metadata of the transaction writing a
// DocWriteBatch as intents.
type TxnMeta struct {
	TxnID    [16]byte
	Priority uint64
	StartHT  types.HybridTime
}

// IntentRecord is the persisted form of a single provisional write made by
// an in-flight transaction.
type IntentRecord struct {
	TxnID [16]byte `json:"txn_id"`
	Key   []byte   `json:"key"`
	Value []byte   `json:"value"`
}

// Engine is the per-tablet document storage engine: it applies committed
// writes to rocks.RegularDB and stages/resolves transactional writes
// through rocks.IntentsDB.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Engine struct {
	store  rocks.Store
	nextHT atomic.Uint64
}

// NewEngine returns an Engine backed by store. Returns an error if store is
// nil.
func NewEngine(store rocks.Store) (*Engine, error) {
	return &Engine{}, nil
}

// ApplyNonTransactional applies batch directly to the regular database,
// assigning a hybrid timestamp to any mutation whose WriteHT is zero.
//
// Not yet implemented in this scaffold.
func (e *Engine) ApplyNonTransactional(ctx context.Context, batch DocWriteBatch) error {
	return errors.ErrNotImplemented
}

// WriteIntents stages batch as provisional writes owned by txn, recording
// both the intent record and its reverse index entry.
//
// Not yet implemented in this scaffold.
func (e *Engine) WriteIntents(ctx context.Context, txn TxnMeta, batch DocWriteBatch) error {
	return errors.ErrNotImplemented
}

// ApplyIntents moves up to limit of txnID's staged intents into the
// regular database at commitHT, removing them from the intents database.
// done reports whether all intents for txnID have been applied.
//
// Not yet implemented in this scaffold.
func (e *Engine) ApplyIntents(ctx context.Context, txnID [16]byte, commitHT uint64, limit int) (done bool, err error) {
	return false, errors.ErrNotImplemented
}

// RemoveIntents deletes up to limit of txnID's staged intents without
// applying them, for use when txnID aborts. done reports whether all
// intents for txnID have been removed.
//
// Not yet implemented in this scaffold.
func (e *Engine) RemoveIntents(ctx context.Context, txnID [16]byte, limit int) (done bool, err error) {
	return false, errors.ErrNotImplemented
}

// Read returns the latest committed value for key.
//
// Not yet implemented in this scaffold.
func (e *Engine) Read(ctx context.Context, key []byte) ([]byte, bool, error) {
	return nil, false, errors.ErrNotImplemented
}

// ReadAt returns the value for key as of readHT: the newest version with a
// write timestamp at or below readHT.
//
// Not yet implemented in this scaffold.
func (e *Engine) ReadAt(ctx context.Context, key []byte, readHT uint64) ([]byte, bool, error) {
	return nil, false, errors.ErrNotImplemented
}
