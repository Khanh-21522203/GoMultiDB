package ids

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type NodeID string
type TabletID string
type TableID string
type RequestID string

type TxnID [16]byte

func NewTxnID() (TxnID, error) {
	var id TxnID
	_, err := rand.Read(id[:])
	if err != nil {
		return TxnID{}, fmt.Errorf("generate txn id: %w", err)
	}
	return id, nil
}

func MustNewTxnID() TxnID {
	id, err := NewTxnID()
	if err != nil {
		panic(err)
	}
	return id
}

func (t TxnID) String() string {
	return hex.EncodeToString(t[:])
}

func (t TxnID) IsZero() bool {
	var z TxnID
	return t == z
}

func NewRequestID() (RequestID, error) {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		return "", fmt.Errorf("generate request id: %w", err)
	}
	return RequestID(hex.EncodeToString(buf)), nil
}

func MustNewRequestID() RequestID {
	id, err := NewRequestID()
	if err != nil {
		panic(err)
	}
	return id
}
