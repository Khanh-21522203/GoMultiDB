package wal

import (
	"bufio"
	"context"
	"os"
	"sync"

	errors "GoMultiDB/v2/contracts/errors"
	"GoMultiDB/v2/contracts/types"
)

// Entry is a single record appended to the write-ahead log.
type Entry struct {
	OpID       types.OpID `json:"op_id"`
	HybridTime uint64     `json:"hybrid_time"`
	Payload    []byte     `json:"payload"`
}

// appendRequest carries a batch of entries queued for the background
// appender along with a completion callback.
type appendRequest struct {
	entries []Entry
	cb      func(error)
}

// Config configures a Log's on-disk segment layout.
type Config struct {
	// Dir is the directory holding WAL segment files. Required.
	Dir string
	// SegmentFile names the initial segment file, used only when Dir has
	// no existing segments on first startup. Defaults to
	// "segment-000001.wal".
	SegmentFile string
	// MaxSegmentBytes caps a segment's size before rotation. Defaults to
	// 64 MiB.
	MaxSegmentBytes int64
}

// indexPos locates an entry within a specific segment file by byte offset.
type indexPos struct {
	Segment string
	Offset  int64
}

// Log is an append-only, segment-rotating write-ahead log. Appends are
// serialized through a background goroutine so callers can fire-and-forget
// via AppendAsync or block via AppendSync.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented or panic.
type Log struct {
	cfg Config

	mu      sync.RWMutex
	entries []Entry
	index   map[types.OpID]indexPos
	lastOp  types.OpID

	segmentNo int
	f         *os.File
	w         *bufio.Writer
	sizeBytes int64

	appendQ chan appendRequest
	stopCh  chan struct{}
	once    sync.Once
}

// NewLog returns a Log rooted at cfg.Dir, recovering any existing segments
// and opening (or creating) the current segment for appends. Returns an
// error if cfg.Dir is empty or the directory cannot be created.
func NewLog(cfg Config) (*Log, error) {
	return &Log{}, nil
}

// AppendAsync enqueues entries for durable append and invokes cb with the
// result once written, without blocking the caller. Returns an error if the
// internal append queue is full.
//
// Not yet implemented in this scaffold.
func (l *Log) AppendAsync(entries []Entry, cb func(error)) error {
	return errors.ErrNotImplemented
}

// AppendSync appends entries and blocks until they are durably written or
// ctx is done.
//
// Not yet implemented in this scaffold.
func (l *Log) AppendSync(ctx context.Context, entries []Entry) error {
	return errors.ErrNotImplemented
}

// ReadFrom returns up to max entries at or after from, in log order. A
// non-positive max returns all matching entries.
//
// Not yet implemented in this scaffold.
func (l *Log) ReadFrom(from types.OpID, max int) []Entry {
	panic("not implemented")
}

// LastOpID returns the OpID of the most recently appended entry.
//
// Not yet implemented in this scaffold.
func (l *Log) LastOpID() types.OpID {
	panic("not implemented")
}

// Rotate closes the current segment and opens a new one, regardless of the
// current segment's size.
//
// Not yet implemented in this scaffold.
func (l *Log) Rotate() error {
	return errors.ErrNotImplemented
}

// IndexLookup returns the segment file and byte offset at which op was
// written, and whether it was found.
//
// Not yet implemented in this scaffold.
func (l *Log) IndexLookup(op types.OpID) (segment string, offset int64, ok bool) {
	panic("not implemented")
}

// Close flushes and syncs the current segment, stops the background
// appender, and releases file handles. Safe to call multiple times.
//
// Not yet implemented in this scaffold.
func (l *Log) Close() error {
	return errors.ErrNotImplemented
}
