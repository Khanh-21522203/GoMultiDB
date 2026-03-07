package cdc

import (
	"context"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

type BootstrapState string

const (
	BootstrapStateNone       BootstrapState = "NONE"
	BootstrapStateRequired   BootstrapState = "REQUIRED"
	BootstrapStateInProgress BootstrapState = "IN_PROGRESS"
	BootstrapStateComplete   BootstrapState = "COMPLETE"
	BootstrapStateFailed     BootstrapState = "FAILED"
)

type BootstrapStatus struct {
	StreamID   string
	State      BootstrapState
	Reason     string
	UpdatedAt  time.Time
}

func (s *Service) ensureBootstrapMap() {
	if s.bootstrap == nil {
		s.bootstrap = make(map[string]BootstrapStatus)
	}
}

func (s *Service) markBootstrapRequiredLocked(streamID, reason string) {
	s.ensureBootstrapMap()
	s.bootstrap[streamID] = BootstrapStatus{StreamID: streamID, State: BootstrapStateRequired, Reason: reason, UpdatedAt: time.Now().UTC()}
}

func (s *Service) StartBootstrap(ctx context.Context, streamID string) error {
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
	if _, ok := s.streams[streamID]; !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	s.ensureBootstrapMap()
	cur := s.bootstrap[streamID]
	if cur.State == BootstrapStateInProgress {
		return nil
	}
	s.bootstrap[streamID] = BootstrapStatus{StreamID: streamID, State: BootstrapStateInProgress, Reason: cur.Reason, UpdatedAt: time.Now().UTC()}
	return nil
}

func (s *Service) CompleteBootstrap(ctx context.Context, streamID string) error {
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
	if _, ok := s.streams[streamID]; !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	s.ensureBootstrapMap()
	cur := s.bootstrap[streamID]
	s.bootstrap[streamID] = BootstrapStatus{StreamID: streamID, State: BootstrapStateComplete, Reason: cur.Reason, UpdatedAt: time.Now().UTC()}
	return nil
}

func (s *Service) FailBootstrap(ctx context.Context, streamID, reason string) error {
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
	if _, ok := s.streams[streamID]; !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	s.ensureBootstrapMap()
	s.bootstrap[streamID] = BootstrapStatus{StreamID: streamID, State: BootstrapStateFailed, Reason: reason, UpdatedAt: time.Now().UTC()}
	return nil
}

func (s *Service) BootstrapStatus(ctx context.Context, streamID string) (BootstrapStatus, error) {
	select {
	case <-ctx.Done():
		return BootstrapStatus{}, ctx.Err()
	default:
	}
	if streamID == "" {
		return BootstrapStatus{}, dberrors.New(dberrors.ErrInvalidArgument, "stream id is required", false, nil)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.streams[streamID]; !ok {
		return BootstrapStatus{}, dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	if s.bootstrap == nil {
		return BootstrapStatus{StreamID: streamID, State: BootstrapStateNone}, nil
	}
	v, ok := s.bootstrap[streamID]
	if !ok {
		return BootstrapStatus{StreamID: streamID, State: BootstrapStateNone}, nil
	}
	return v, nil
}

func (s *Service) EvaluateRetentionAndMark(ctx context.Context, streamID string, checkpointSeq uint64, retentionFloorSeq uint64) (RetentionStatus, error) {
	status, err := s.EvaluateRetention(ctx, streamID, checkpointSeq, retentionFloorSeq)
	if err != nil {
		return RetentionOK, err
	}
	if status == RetentionBootstrapNeeded {
		s.mu.Lock()
		s.markBootstrapRequiredLocked(streamID, "checkpoint below retention floor")
		s.mu.Unlock()
	}
	return status, nil
}

