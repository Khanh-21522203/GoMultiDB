package types

import (
	"time"

	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/common/versioning"
)

type HybridTime uint64

type OpID struct {
	Term  uint64
	Index uint64
}

func (o OpID) IsZero() bool {
	return o.Term == 0 && o.Index == 0
}

func (o OpID) Less(other OpID) bool {
	if o.Term != other.Term {
		return o.Term < other.Term
	}
	return o.Index < other.Index
}

func (o OpID) Equal(other OpID) bool {
	return o.Term == other.Term && o.Index == other.Index
}

type RequestEnvelope struct {
	RequestID   ids.RequestID
	SourceNode  ids.NodeID
	SentAt      time.Time
	ContractVer uint32
}

func NewRequestEnvelope(node ids.NodeID, requestID ids.RequestID) RequestEnvelope {
	return RequestEnvelope{
		RequestID:   requestID,
		SourceNode:  node,
		SentAt:      time.Now().UTC(),
		ContractVer: versioning.CurrentContractVersion,
	}
}
