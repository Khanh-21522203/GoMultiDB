package cdc

import (
	"context"
	"sync"
	"time"

	dberrors "GoMultiDB/v2/contracts/errors"
)

// FileCheckpointStore persists CDC checkpoints to a local JSON file,
// keyed by stream and tablet ID.
//
// Scaffold stub: all methods are unimplemented.
type FileCheckpointStore struct {
	mu          sync.RWMutex
	path        string
	checkpoints map[string]map[string]Checkpoint
	lastUpdated time.Time
}

// NewFileCheckpointStore returns a FileCheckpointStore backed by the file
// at path, loading any existing checkpoints from disk.
func NewFileCheckpointStore(path string) (*FileCheckpointStore, error) {
	return &FileCheckpointStore{}, nil
}

// AdvanceCheckpoint persists cp, rejecting any sequence regression for
// the same (StreamID, TabletID) pair.
//
// Not yet implemented in this scaffold.
func (s *FileCheckpointStore) AdvanceCheckpoint(ctx context.Context, cp Checkpoint) error {
	return dberrors.ErrNotImplemented
}

// GetCheckpoint returns the persisted Checkpoint for (streamID, tabletID),
// or a zero-sequence Checkpoint if none has been recorded.
//
// Not yet implemented in this scaffold.
func (s *FileCheckpointStore) GetCheckpoint(ctx context.Context, streamID, tabletID string) (Checkpoint, error) {
	return Checkpoint{}, dberrors.ErrNotImplemented
}

// LastUpdated returns the timestamp of the most recent checkpoint write.
//
// Not yet implemented in this scaffold.
func (s *FileCheckpointStore) LastUpdated(ctx context.Context) (time.Time, error) {
	return time.Time{}, dberrors.ErrNotImplemented
}
