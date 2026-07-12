package ids

import (
	"encoding/hex"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// NodeID identifies a single node (master or tablet server) in the cluster.
type NodeID string

// TabletID identifies a single tablet: a shard of a table's data.
type TabletID string

// TableID identifies a table within the catalog.
type TableID string

// RequestID identifies a single client request, used for tracing and
// idempotency.
type RequestID string

// TxnID is the fixed-size, randomly generated identifier for a
// transaction.
type TxnID [16]byte

// NewTxnID generates a new random TxnID.
//
// Not yet implemented in this scaffold.
func NewTxnID() (TxnID, error) {
	return TxnID{}, dberrors.ErrNotImplemented
}

// MustNewTxnID generates a new random TxnID, panicking if generation
// fails.
//
// Not yet implemented in this scaffold.
func MustNewTxnID() TxnID {
	panic("not implemented")
}

// String renders the TxnID as a lowercase hex string.
func (t TxnID) String() string {
	return hex.EncodeToString(t[:])
}

// IsZero reports whether t is the zero-value TxnID.
func (t TxnID) IsZero() bool {
	var z TxnID
	return t == z
}

// NewRequestID generates a new random RequestID.
//
// Not yet implemented in this scaffold.
func NewRequestID() (RequestID, error) {
	return "", dberrors.ErrNotImplemented
}

// MustNewRequestID generates a new random RequestID, panicking if
// generation fails.
//
// Not yet implemented in this scaffold.
func MustNewRequestID() RequestID {
	panic("not implemented")
}
