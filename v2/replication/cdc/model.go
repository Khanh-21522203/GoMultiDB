package cdc

import "time"

// Event is a single change-data-capture record produced for a tablet
// within a stream.
type Event struct {
	StreamID     string
	TabletID     string
	Sequence     uint64
	TimestampUTC time.Time
	Payload      []byte
}

// Checkpoint records the last sequence number consumed for a
// (StreamID, TabletID) pair.
type Checkpoint struct {
	StreamID  string
	TabletID  string
	Sequence  uint64
	Timestamp time.Time
}

// PollRequest requests events for a tablet within a stream, after a given
// sequence number, up to MaxRecords.
type PollRequest struct {
	StreamID   string
	TabletID   string
	AfterSeq   uint64
	MaxRecords int
}

// PollResponse is the result of a poll: the events found, and the latest
// sequence number seen (which may exceed the last returned event's
// sequence when no events matched).
type PollResponse struct {
	Events     []Event
	LatestSeen uint64
}
