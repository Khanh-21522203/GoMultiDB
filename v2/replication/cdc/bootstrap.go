package cdc

import (
	"context"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// BootstrapState is the lifecycle state of a stream's re-bootstrap
// process.
type BootstrapState string

// BootstrapState values.
const (
	BootstrapStateNone       BootstrapState = "NONE"
	BootstrapStateRequired   BootstrapState = "REQUIRED"
	BootstrapStateInProgress BootstrapState = "IN_PROGRESS"
	BootstrapStateComplete   BootstrapState = "COMPLETE"
	BootstrapStateFailed     BootstrapState = "FAILED"
)

// BootstrapStatus reports a stream's current bootstrap state and the
// reason for its last transition.
type BootstrapStatus struct {
	StreamID  string
	State     BootstrapState
	Reason    string
	UpdatedAt time.Time
}

// StartBootstrap transitions streamID into BootstrapStateInProgress.
//
// Not yet implemented in this scaffold.
func (s *Service) StartBootstrap(ctx context.Context, streamID string) error {
	return dberrors.ErrNotImplemented
}

// CompleteBootstrap transitions streamID into BootstrapStateComplete.
//
// Not yet implemented in this scaffold.
func (s *Service) CompleteBootstrap(ctx context.Context, streamID string) error {
	return dberrors.ErrNotImplemented
}

// FailBootstrap transitions streamID into BootstrapStateFailed, recording
// reason.
//
// Not yet implemented in this scaffold.
func (s *Service) FailBootstrap(ctx context.Context, streamID, reason string) error {
	return dberrors.ErrNotImplemented
}

// BootstrapStatus returns the current BootstrapStatus for streamID,
// defaulting to BootstrapStateNone if no bootstrap has been recorded.
//
// Not yet implemented in this scaffold.
func (s *Service) BootstrapStatus(ctx context.Context, streamID string) (BootstrapStatus, error) {
	return BootstrapStatus{}, dberrors.ErrNotImplemented
}

// EvaluateRetentionAndMark evaluates retention for streamID and, if the
// checkpoint has fallen behind the retention floor, marks the stream as
// requiring bootstrap.
//
// Not yet implemented in this scaffold.
func (s *Service) EvaluateRetentionAndMark(ctx context.Context, streamID string, checkpointSeq uint64, retentionFloorSeq uint64) (RetentionStatus, error) {
	return RetentionOK, dberrors.ErrNotImplemented
}
