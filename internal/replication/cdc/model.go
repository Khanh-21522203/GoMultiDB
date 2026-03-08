package cdc

import "time"

type Event struct {
	StreamID     string
	TabletID     string
	Sequence     uint64
	TimestampUTC time.Time
	Payload      []byte
}

type Checkpoint struct {
	StreamID  string
	TabletID  string
	Sequence  uint64
	Timestamp time.Time
}

type PollRequest struct {
	StreamID   string
	TabletID   string
	AfterSeq   uint64
	MaxRecords int
}

type PollResponse struct {
	Events     []Event
	LatestSeen uint64
}
