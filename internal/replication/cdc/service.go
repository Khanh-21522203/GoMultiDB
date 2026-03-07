package cdc

import (
	"context"
	"sort"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

type Producer interface {
	Read(ctx context.Context, req ProducerReadRequest) (ProducerReadResponse, error)
}

type ProducerReadRequest struct {
	StreamID   string
	TabletID   string
	AfterSeq   uint64
	MaxRecords int
}

type ProducerReadResponse struct {
	Events     []Event
	LatestSeen uint64
}

type StoreProducer struct {
	store *Store
}

func NewStoreProducer(store *Store) (*StoreProducer, error) {
	if store == nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "cdc store is required", false, nil)
	}
	return &StoreProducer{store: store}, nil
}

func (p *StoreProducer) Read(ctx context.Context, req ProducerReadRequest) (ProducerReadResponse, error) {
	poll, err := p.store.Poll(ctx, PollRequest{StreamID: req.StreamID, TabletID: req.TabletID, AfterSeq: req.AfterSeq, MaxRecords: req.MaxRecords})
	if err != nil {
		return ProducerReadResponse{}, err
	}
	return ProducerReadResponse{Events: poll.Events, LatestSeen: poll.LatestSeen}, nil
}

type Service struct {
	mu         sync.RWMutex
	store      *Store
	producer   Producer
	streams    map[string]streamMeta
	children   map[string][]string
	bootstrap  map[string]BootstrapStatus
}

type streamMeta struct {
	ID        string
	TabletID  string
	CreatedAt time.Time
}

type GetChangesRequest struct {
	StreamID   string
	AfterSeq   uint64
	MaxRecords int
}

type GetChangesResponse struct {
	TabletID   string
	Events     []Event
	LatestSeen uint64
}

func NewService(store *Store) (*Service, error) {
	if store == nil {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "cdc store is required", false, nil)
	}
	producer, err := NewStoreProducer(store)
	if err != nil {
		return nil, err
	}
	return &Service{store: store, producer: producer, streams: make(map[string]streamMeta), children: make(map[string][]string), bootstrap: make(map[string]BootstrapStatus)}, nil
}

func (s *Service) SetProducer(p Producer) error {
	if p == nil {
		return dberrors.New(dberrors.ErrInvalidArgument, "producer is required", false, nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.producer = p
	return nil
}

func (s *Service) CreateStream(ctx context.Context, streamID, tabletID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if streamID == "" || tabletID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.streams[streamID]; ok {
		return nil
	}
	s.streams[streamID] = streamMeta{ID: streamID, TabletID: tabletID, CreatedAt: time.Now().UTC()}
	return nil
}

func (s *Service) DeleteStream(ctx context.Context, streamID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if streamID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id is required", false, nil)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.streams, streamID)
	delete(s.children, streamID)
	return nil
}

func (s *Service) SetCheckpoint(ctx context.Context, cp Checkpoint) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.RLock()
	m, ok := s.streams[cp.StreamID]
	s.mu.RUnlock()
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	if cp.TabletID != m.TabletID {
		return dberrors.New(dberrors.ErrInvalidArgument, "tablet mismatch for stream", false, nil)
	}
	return s.store.AdvanceCheckpoint(ctx, cp)
}

func (s *Service) GetChanges(ctx context.Context, req GetChangesRequest) (GetChangesResponse, error) {
	select {
	case <-ctx.Done():
		return GetChangesResponse{}, ctx.Err()
	default:
	}
	if req.StreamID == "" {
		return GetChangesResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "stream id is required", false, nil)
	}
	s.mu.RLock()
	m, ok := s.streams[req.StreamID]
	p := s.producer
	s.mu.RUnlock()
	if !ok {
		return GetChangesResponse{}, dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	if p == nil {
		return GetChangesResponse{}, dberrors.New(dberrors.ErrInternal, "producer is not configured", false, nil)
	}

	out, err := p.Read(ctx, ProducerReadRequest{StreamID: req.StreamID, TabletID: m.TabletID, AfterSeq: req.AfterSeq, MaxRecords: req.MaxRecords})
	if err != nil {
		return GetChangesResponse{}, err
	}
	return GetChangesResponse{TabletID: m.TabletID, Events: out.Events, LatestSeen: out.LatestSeen}, nil
}

func (s *Service) ListStreams(ctx context.Context) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.streams))
	for id := range s.streams {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}
