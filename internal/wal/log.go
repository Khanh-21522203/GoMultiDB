package wal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"GoMultiDB/internal/common/types"
)

type Entry struct {
	OpID       types.OpID `json:"op_id"`
	HybridTime uint64     `json:"hybrid_time"`
	Payload    []byte     `json:"payload"`
}

type diskRecord struct {
	Entry Entry  `json:"entry"`
	CRC32 uint32 `json:"crc32"`
}

type appendRequest struct {
	entries []Entry
	cb      func(error)
}

type Config struct {
	Dir             string
	SegmentFile     string
	MaxSegmentBytes int64
}

type indexPos struct {
	Segment string
	Offset  int64
}

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

func NewLog(cfg Config) (*Log, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("wal dir is required")
	}
	if cfg.SegmentFile == "" {
		cfg.SegmentFile = "segment-000001.wal"
	}
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = 64 << 20
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}

	log := &Log{
		cfg:     cfg,
		appendQ: make(chan appendRequest, 256),
		stopCh:  make(chan struct{}),
		index:   make(map[types.OpID]indexPos),
	}
	if err := log.recoverAll(); err != nil {
		return nil, err
	}
	if err := log.openCurrentSegment(); err != nil {
		return nil, err
	}
	go log.runAppender()
	return log, nil
}

func (l *Log) runAppender() {
	for {
		select {
		case req := <-l.appendQ:
			err := l.appendAndSync(req.entries)
			if req.cb != nil {
				req.cb(err)
			}
		case <-l.stopCh:
			return
		}
	}
}

func (l *Log) AppendAsync(entries []Entry, cb func(error)) error {
	if len(entries) == 0 {
		if cb != nil {
			cb(nil)
		}
		return nil
	}
	select {
	case l.appendQ <- appendRequest{entries: entries, cb: cb}:
		return nil
	default:
		return errors.New("wal append queue full")
	}
}

func (l *Log) AppendSync(ctx context.Context, entries []Entry) error {
	done := make(chan error, 1)
	if err := l.AppendAsync(entries, func(err error) { done <- err }); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Log) ReadFrom(from types.OpID, max int) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if max <= 0 {
		max = len(l.entries)
	}
	out := make([]Entry, 0, max)
	for _, e := range l.entries {
		if from.IsZero() || from.Less(e.OpID) || from.Equal(e.OpID) {
			out = append(out, e)
			if len(out) == max {
				break
			}
		}
	}
	return out
}

func (l *Log) LastOpID() types.OpID {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastOp
}

func (l *Log) Rotate() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rotateLocked()
}

func (l *Log) IndexLookup(op types.OpID) (segment string, offset int64, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	p, exists := l.index[op]
	if !exists {
		return "", 0, false
	}
	return p.Segment, p.Offset, true
}

func (l *Log) HasOpID(op types.OpID) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, ok := l.index[op]
	return ok
}

func (l *Log) Close() error {
	var err error
	l.once.Do(func() {
		close(l.stopCh)
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.w != nil {
			if e := l.w.Flush(); e != nil {
				err = e
			}
		}
		if l.f != nil {
			if e := l.f.Sync(); e != nil && err == nil {
				err = e
			}
			if e := l.f.Close(); e != nil && err == nil {
				err = e
			}
		}
	})
	return err
}

func (l *Log) appendAndSync(entries []Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, e := range entries {
		r, err := marshalRecord(e)
		if err != nil {
			return err
		}
		rec := append(r, '\n')
		if l.sizeBytes+int64(len(rec)) > l.cfg.MaxSegmentBytes {
			if err := l.rotateLocked(); err != nil {
				return err
			}
		}
		offset := l.sizeBytes
		if _, err := l.w.Write(rec); err != nil {
			return err
		}
		l.sizeBytes += int64(len(rec))
		l.entries = append(l.entries, e)
		l.lastOp = e.OpID
		l.index[e.OpID] = indexPos{Segment: currentSegmentName(l.segmentNo), Offset: offset}
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *Log) recoverAll() error {
	segments, err := listSegments(l.cfg.Dir)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		segNo, err := parseSegmentNo(l.cfg.SegmentFile)
		if err != nil {
			return err
		}
		l.segmentNo = segNo
		return nil
	}

	for _, seg := range segments {
		path := filepath.Join(l.cfg.Dir, seg)
		validUntil, segEntries, err := recoverSegment(path)
		if err != nil {
			return err
		}
		if validUntil < 0 {
			validUntil = 0
		}
		if err := os.Truncate(path, validUntil); err != nil {
			return fmt.Errorf("truncate corrupted segment %s: %w", seg, err)
		}

		var offset int64
		for _, e := range segEntries {
			rec, _ := marshalRecord(e)
			l.entries = append(l.entries, e)
			l.lastOp = e.OpID
			l.index[e.OpID] = indexPos{Segment: seg, Offset: offset}
			offset += int64(len(rec) + 1)
		}
	}

	last := segments[len(segments)-1]
	segNo, err := parseSegmentNo(last)
	if err != nil {
		return err
	}
	l.segmentNo = segNo
	return nil
}

func recoverSegment(path string) (validUntil int64, entries []Entry, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	var offset int64
	for s.Scan() {
		line := append([]byte(nil), s.Bytes()...)
		if len(line) == 0 {
			offset++
			validUntil = offset
			continue
		}
		rec, ok, derr := decodeRecord(line)
		if derr != nil {
			return 0, nil, derr
		}
		if !ok {
			return offset, entries, nil
		}
		entries = append(entries, rec.Entry)
		offset += int64(len(line) + 1)
		validUntil = offset
	}
	if err := s.Err(); err != nil {
		return 0, nil, err
	}
	return validUntil, entries, nil
}

func (l *Log) openCurrentSegment() error {
	segName := currentSegmentName(l.segmentNo)
	path := filepath.Join(l.cfg.Dir, segName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open wal segment: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat wal segment: %w", err)
	}
	l.f = f
	l.w = bufio.NewWriter(f)
	l.sizeBytes = info.Size()
	return nil
}

func (l *Log) rotateLocked() error {
	if l.w != nil {
		if err := l.w.Flush(); err != nil {
			return err
		}
	}
	if l.f != nil {
		if err := l.f.Sync(); err != nil {
			return err
		}
		if err := l.f.Close(); err != nil {
			return err
		}
	}
	l.segmentNo++
	l.sizeBytes = 0
	return l.openCurrentSegment()
}

func listSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "segment-") && strings.HasSuffix(name, ".wal") {
			segs = append(segs, name)
		}
	}
	sort.Slice(segs, func(i, j int) bool {
		a, _ := parseSegmentNo(segs[i])
		b, _ := parseSegmentNo(segs[j])
		return a < b
	})
	return segs, nil
}

func parseSegmentNo(name string) (int, error) {
	if !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".wal") {
		return 0, fmt.Errorf("invalid segment name: %s", name)
	}
	n := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".wal")
	v, err := strconv.Atoi(n)
	if err != nil {
		return 0, fmt.Errorf("parse segment number %s: %w", name, err)
	}
	if v <= 0 {
		return 0, fmt.Errorf("invalid segment number in %s", name)
	}
	return v, nil
}

func currentSegmentName(no int) string {
	return fmt.Sprintf("segment-%06d.wal", no)
}

func marshalRecord(e Entry) ([]byte, error) {
	entryBytes, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	rec := diskRecord{Entry: e, CRC32: crc32.ChecksumIEEE(entryBytes)}
	return json.Marshal(rec)
}

func decodeRecord(line []byte) (diskRecord, bool, error) {
	var rec diskRecord
	if err := json.Unmarshal(line, &rec); err != nil {
		return diskRecord{}, false, nil
	}
	entryBytes, err := json.Marshal(rec.Entry)
	if err != nil {
		return diskRecord{}, false, err
	}
	crc := crc32.ChecksumIEEE(entryBytes)
	if crc != rec.CRC32 {
		return diskRecord{}, false, nil
	}
	return rec, true, nil
}
