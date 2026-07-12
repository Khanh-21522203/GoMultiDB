package cdc

import (
	"context"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// RetentionStatus classifies whether a stream's checkpoint is still
// within its retention window.
type RetentionStatus string

// RetentionStatus values.
const (
	// RetentionOK indicates the checkpoint is within the retention floor.
	RetentionOK RetentionStatus = "OK"
	// RetentionBootstrapNeeded indicates the checkpoint has fallen behind
	// the retention floor and the stream must be re-bootstrapped.
	RetentionBootstrapNeeded RetentionStatus = "BOOTSTRAP_REQUIRED"
)

// SplitRemap describes a parent tablet splitting into one or more child
// tablets for CDC purposes.
type SplitRemap struct {
	ParentTabletID string
	ChildTabletIDs []string
}

// EvaluateRetention reports whether checkpointSeq has fallen behind
// retentionFloorSeq for streamID, indicating that a re-bootstrap is
// required.
//
// Not yet implemented in this scaffold.
func (s *Service) EvaluateRetention(ctx context.Context, streamID string, checkpointSeq uint64, retentionFloorSeq uint64) (RetentionStatus, error) {
	return RetentionOK, dberrors.ErrNotImplemented
}

// RegisterSplitRemap records that streamID's parent tablet has split into
// the tablets in remap, and pins the stream's tablet assignment to a
// child.
//
// Not yet implemented in this scaffold.
func (s *Service) RegisterSplitRemap(ctx context.Context, streamID string, remap SplitRemap) error {
	return dberrors.ErrNotImplemented
}

// SplitChildren returns the child tablet IDs previously registered for
// streamID via RegisterSplitRemap.
//
// Not yet implemented in this scaffold.
func (s *Service) SplitChildren(ctx context.Context, streamID string) ([]string, error) {
	return nil, dberrors.ErrNotImplemented
}
