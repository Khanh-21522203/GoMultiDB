package cdc

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

type Metrics struct {
	PollRequests       uint64
	PollReturnedEvents uint64
	CheckpointAdvances uint64
	CheckpointNoops    uint64
	AppendEvents       uint64
	AppendNoops        uint64
}

type Status struct {
	Streams     int
	Tablets     int
	Metrics     Metrics
	LastUpdated time.Time
}

type LagSnapshot struct {
	StreamID    string
	TabletID    string
	LatestSeq   uint64
	Checkpoint  uint64
	LagEvents   uint64
	CollectedAt time.Time
}

type Store struct {
	mu          sync.RWMutex
	events      map[string]map[string][]Event
	checkpoints map[string]map[string]Checkpoint
	metrics     Metrics
	lastUpdated time.Time
}

func NewStore() *Store {
	return &Store{
		events:      make(map[string]map[string][]Event),
		checkpoints: make(map[string]map[string]Checkpoint),
		lastUpdated: time.Now().UTC(),
	}
}

func (s *Store) AppendEvent(ctx context.Context, ev Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if ev.StreamID == "" || ev.TabletID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}
	if ev.Sequence == 0 {
		return dberrors.New(dberrors.ErrInvalidArgument, "sequence must be >= 1", false, nil)
	}
	if ev.TimestampUTC.IsZero() {
		ev.TimestampUTC = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.events[ev.StreamID]; !ok {
		s.events[ev.StreamID] = make(map[string][]Event)
	}
	curr := s.events[ev.StreamID][ev.TabletID]
	if len(curr) > 0 {
		last := curr[len(curr)-1]
		if ev.Sequence <= last.Sequence {
			if ev.Sequence == last.Sequence {
				s.metrics.AppendNoops++
				s.lastUpdated = time.Now().UTC()
				log.Printf("cdc append noop stream=%s tablet=%s seq=%d", ev.StreamID, ev.TabletID, ev.Sequence)
				return nil
			}
			return dberrors.New(dberrors.ErrConflict, "event sequence must be monotonic", false, nil)
		}
	}
	s.events[ev.StreamID][ev.TabletID] = append(curr, ev)
	s.metrics.AppendEvents++
	s.lastUpdated = time.Now().UTC()
	log.Printf("cdc append stream=%s tablet=%s seq=%d", ev.StreamID, ev.TabletID, ev.Sequence)
	return nil
}

func (s *Store) Poll(ctx context.Context, req PollRequest) (PollResponse, error) {
	select {
	case <-ctx.Done():
		return PollResponse{}, ctx.Err()
	default:
	}
	if req.StreamID == "" || req.TabletID == "" {
		return PollResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}
	if req.MaxRecords <= 0 {
		req.MaxRecords = 100
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tabletEvents := s.events[req.StreamID][req.TabletID]
	s.metrics.PollRequests++
	if len(tabletEvents) == 0 {
		s.lastUpdated = time.Now().UTC()
		return PollResponse{Events: nil, LatestSeen: req.AfterSeq}, nil
	}

	out := make([]Event, 0, req.MaxRecords)
	latest := req.AfterSeq
	for _, ev := range tabletEvents {
		if ev.Sequence <= req.AfterSeq {
			continue
		}
		out = append(out, ev)
		latest = ev.Sequence
		if len(out) >= req.MaxRecords {
			break
		}
	}
	s.metrics.PollReturnedEvents += uint64(len(out))
	s.lastUpdated = time.Now().UTC()
	return PollResponse{Events: out, LatestSeen: latest}, nil
}

func (s *Store) AdvanceCheckpoint(ctx context.Context, cp Checkpoint) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if cp.StreamID == "" || cp.TabletID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.checkpoints[cp.StreamID]; !ok {
		s.checkpoints[cp.StreamID] = make(map[string]Checkpoint)
	}
	curr, ok := s.checkpoints[cp.StreamID][cp.TabletID]
	if ok {
		if cp.Sequence < curr.Sequence {
			return dberrors.New(dberrors.ErrConflict, "checkpoint sequence regression is not allowed", false, nil)
		}
		if cp.Sequence == curr.Sequence {
			s.metrics.CheckpointNoops++
			s.lastUpdated = time.Now().UTC()
			log.Printf("cdc checkpoint noop stream=%s tablet=%s seq=%d", cp.StreamID, cp.TabletID, cp.Sequence)
			return nil
		}
	}
	s.checkpoints[cp.StreamID][cp.TabletID] = cp
	s.metrics.CheckpointAdvances++
	s.lastUpdated = time.Now().UTC()
	log.Printf("cdc checkpoint advance stream=%s tablet=%s seq=%d", cp.StreamID, cp.TabletID, cp.Sequence)
	return nil
}

func (s *Store) GetCheckpoint(ctx context.Context, streamID, tabletID string) (Checkpoint, error) {
	select {
	case <-ctx.Done():
		return Checkpoint{}, ctx.Err()
	default:
	}
	if streamID == "" || tabletID == "" {
		return Checkpoint{}, dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	cp, ok := s.checkpoints[streamID][tabletID]
	if !ok {
		return Checkpoint{StreamID: streamID, TabletID: tabletID, Sequence: 0}, nil
	}
	return cp, nil
}

func (s *Store) Streams(ctx context.Context) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.events))
	for streamID := range s.events {
		out = append(out, streamID)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) Status(ctx context.Context) (Status, error) {
	select {
	case <-ctx.Done():
		return Status{}, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	tablets := 0
	for _, byTablet := range s.events {
		tablets += len(byTablet)
	}
	return Status{
		Streams:     len(s.events),
		Tablets:     tablets,
		Metrics:     s.metrics,
		LastUpdated: s.lastUpdated,
	}, nil
}

func (s *Store) LagSnapshot(ctx context.Context, streamID, tabletID string) (LagSnapshot, error) {
	select {
	case <-ctx.Done():
		return LagSnapshot{}, ctx.Err()
	default:
	}
	if streamID == "" || tabletID == "" {
		return LagSnapshot{}, dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	latest := uint64(0)
	evs := s.events[streamID][tabletID]
	if len(evs) > 0 {
		latest = evs[len(evs)-1].Sequence
	}
	cp := uint64(0)
	if byTablet, ok := s.checkpoints[streamID]; ok {
		if c, ok := byTablet[tabletID]; ok {
			cp = c.Sequence
		}
	}
	lag := uint64(0)
	if latest > cp {
		lag = latest - cp
	}
	return LagSnapshot{
		StreamID:    streamID,
		TabletID:    tabletID,
		LatestSeq:   latest,
		Checkpoint:  cp,
		LagEvents:   lag,
		CollectedAt: time.Now().UTC(),
	}, nil
}
