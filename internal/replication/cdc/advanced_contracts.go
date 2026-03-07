package cdc

import (
	"context"
	"sort"

	dberrors "GoMultiDB/internal/common/errors"
)

type RetentionStatus string

const (
	RetentionOK              RetentionStatus = "OK"
	RetentionBootstrapNeeded RetentionStatus = "BOOTSTRAP_REQUIRED"
)

type SplitRemap struct {
	ParentTabletID string
	ChildTabletIDs []string
}

func (s *Service) EvaluateRetention(ctx context.Context, streamID string, checkpointSeq uint64, retentionFloorSeq uint64) (RetentionStatus, error) {
	select {
	case <-ctx.Done():
		return RetentionOK, ctx.Err()
	default:
	}
	if streamID == "" {
		return RetentionOK, dberrors.New(dberrors.ErrInvalidArgument, "stream id is required", false, nil)
	}
	s.mu.RLock()
	_, ok := s.streams[streamID]
	s.mu.RUnlock()
	if !ok {
		return RetentionOK, dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	if checkpointSeq < retentionFloorSeq {
		return RetentionBootstrapNeeded, nil
	}
	return RetentionOK, nil
}

func (s *Service) RegisterSplitRemap(ctx context.Context, streamID string, remap SplitRemap) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if streamID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id is required", false, nil)
	}
	if remap.ParentTabletID == "" || len(remap.ChildTabletIDs) == 0 {
		return dberrors.New(dberrors.ErrInvalidArgument, "parent and child tablet ids are required", false, nil)
	}
	children := append([]string(nil), remap.ChildTabletIDs...)
	sort.Strings(children)

	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.streams[streamID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	if m.TabletID != remap.ParentTabletID {
		return dberrors.New(dberrors.ErrInvalidArgument, "parent tablet does not match stream tablet", false, nil)
	}
	// v2 scaffold semantics: keep parent stream id; pin primary child to first sorted child;
	// retain full child fanout metadata for future parallel child-stream processing.
	m.TabletID = children[0]
	s.streams[streamID] = m
	s.children[streamID] = children
	return nil
}

func (s *Service) SplitChildren(ctx context.Context, streamID string) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if streamID == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "stream id is required", false, nil)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.streams[streamID]; !ok {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	out := append([]string(nil), s.children[streamID]...)
	return out, nil
}
