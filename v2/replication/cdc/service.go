package cdc

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// Producer reads CDC events for a tablet from some underlying source,
// local or remote.
type Producer interface {
	Read(ctx context.Context, req ProducerReadRequest) (ProducerReadResponse, error)
}

// ProducerReadRequest requests events for a tablet within a stream, after
// a given sequence number, up to MaxRecords.
type ProducerReadRequest struct {
	StreamID   string
	TabletID   string
	AfterSeq   uint64
	MaxRecords int
}

// ProducerReadResponse is the result of a Producer.Read call.
type ProducerReadResponse struct {
	Events     []Event
	LatestSeen uint64
}

// StoreProducer is a Producer backed directly by a local Store.
//
// Scaffold stub: Read returns errors.ErrNotImplemented.
type StoreProducer struct {
	store *Store
}

// NewStoreProducer returns a StoreProducer reading from store.
func NewStoreProducer(store *Store) (*StoreProducer, error) {
	return &StoreProducer{}, nil
}

// Read polls store for events matching req.
//
// Not yet implemented in this scaffold.
func (p *StoreProducer) Read(ctx context.Context, req ProducerReadRequest) (ProducerReadResponse, error) {
	return ProducerReadResponse{}, dberrors.ErrNotImplemented
}

// streamMeta is the internal record of a stream's identity and current
// tablet assignment.
type streamMeta struct {
	ID        string
	TabletID  string
	CreatedAt time.Time
}

// Service exposes stream lifecycle and change-polling operations over a
// Store, delegating change reads to a pluggable Producer.
//
// Scaffold stub: all methods are unimplemented.
type Service struct {
	mu        sync.RWMutex
	store     *Store
	producer  Producer
	streams   map[string]streamMeta
	children  map[string][]string
	bootstrap map[string]BootstrapStatus
}

// GetChangesRequest requests changes for a stream, after a given
// sequence number, up to MaxRecords.
type GetChangesRequest struct {
	StreamID   string
	AfterSeq   uint64
	MaxRecords int
}

// GetChangesResponse is the result of a GetChanges call.
type GetChangesResponse struct {
	TabletID   string
	Events     []Event
	LatestSeen uint64
}

// NewService returns a Service backed by store, with a default
// StoreProducer wired in as its Producer.
func NewService(store *Store) (*Service, error) {
	return &Service{}, nil
}

// SetProducer replaces the Service's Producer.
//
// Not yet implemented in this scaffold.
func (s *Service) SetProducer(p Producer) error {
	return dberrors.ErrNotImplemented
}

// CreateStream registers a new stream over tabletID. Idempotent if the
// stream already exists.
//
// Not yet implemented in this scaffold.
func (s *Service) CreateStream(ctx context.Context, streamID, tabletID string) error {
	return dberrors.ErrNotImplemented
}

// DeleteStream removes a stream and any associated split-child metadata.
//
// Not yet implemented in this scaffold.
func (s *Service) DeleteStream(ctx context.Context, streamID string) error {
	return dberrors.ErrNotImplemented
}

// SetCheckpoint advances the checkpoint for cp's stream, validating that
// cp's tablet matches the stream's current tablet assignment.
//
// Not yet implemented in this scaffold.
func (s *Service) SetCheckpoint(ctx context.Context, cp Checkpoint) error {
	return dberrors.ErrNotImplemented
}

// GetChanges reads changes for req.StreamID via the configured Producer.
//
// Not yet implemented in this scaffold.
func (s *Service) GetChanges(ctx context.Context, req GetChangesRequest) (GetChangesResponse, error) {
	return GetChangesResponse{}, dberrors.ErrNotImplemented
}

// ListStreams returns the IDs of all registered streams, sorted.
//
// Not yet implemented in this scaffold.
func (s *Service) ListStreams(ctx context.Context) ([]string, error) {
	return nil, dberrors.ErrNotImplemented
}
