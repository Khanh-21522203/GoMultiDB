package types

import (
	"time"

	"GoMultiDB/v2/contracts/ids"
	"GoMultiDB/v2/contracts/versioning"
)

// HybridTime is a hybrid logical clock timestamp used to order events
// across nodes without relying on synchronized wall clocks.
type HybridTime uint64

// OpID identifies a single replicated operation by its Raft-style term and
// log index.
type OpID struct {
	Term  uint64
	Index uint64
}

// IsZero reports whether o is the zero-value OpID.
func (o OpID) IsZero() bool {
	return o.Term == 0 && o.Index == 0
}

// Less reports whether o sorts before other in log order: first by Term,
// then by Index.
func (o OpID) Less(other OpID) bool {
	if o.Term != other.Term {
		return o.Term < other.Term
	}
	return o.Index < other.Index
}

// Equal reports whether o and other identify the same operation.
func (o OpID) Equal(other OpID) bool {
	return o.Term == other.Term && o.Index == other.Index
}

// RequestEnvelope wraps every request crossing a GoMultiDB v2 module or
// node boundary with tracing and contract-versioning metadata.
type RequestEnvelope struct {
	RequestID   ids.RequestID
	SourceNode  ids.NodeID
	SentAt      time.Time
	ContractVer uint32
}

// NewRequestEnvelope builds a RequestEnvelope for a request originating at
// node, stamping it with the current time and the current contract
// version.
func NewRequestEnvelope(node ids.NodeID, requestID ids.RequestID) RequestEnvelope {
	return RequestEnvelope{
		RequestID:   requestID,
		SourceNode:  node,
		SentAt:      time.Now().UTC(),
		ContractVer: versioning.CurrentContractVersion,
	}
}
