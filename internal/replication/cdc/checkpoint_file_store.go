package cdc

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

type fileCheckpointHooks struct {
	writeFile func(name string, data []byte, perm os.FileMode) error
	rename    func(oldPath, newPath string) error
}

type FileCheckpointStore struct {
	mu          sync.RWMutex
	path        string
	checkpoints map[string]map[string]Checkpoint
	lastUpdated time.Time
	hooks       fileCheckpointHooks
}

func NewFileCheckpointStore(path string) (*FileCheckpointStore, error) {
	if path == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "checkpoint file path is required", false, nil)
	}
	store := &FileCheckpointStore{
		path:        path,
		checkpoints: make(map[string]map[string]Checkpoint),
		lastUpdated: time.Now().UTC(),
		hooks: fileCheckpointHooks{
			writeFile: os.WriteFile,
			rename:    os.Rename,
		},
	}
	if err := store.loadFromDisk(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileCheckpointStore) AdvanceCheckpoint(ctx context.Context, cp Checkpoint) error {
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
			return nil
		}
	}
	s.checkpoints[cp.StreamID][cp.TabletID] = cp
	s.lastUpdated = time.Now().UTC()
	return s.saveToDiskLocked()
}

func (s *FileCheckpointStore) GetCheckpoint(ctx context.Context, streamID, tabletID string) (Checkpoint, error) {
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

func (s *FileCheckpointStore) LastUpdated(ctx context.Context) (time.Time, error) {
	select {
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastUpdated, nil
}

func (s *FileCheckpointStore) loadFromDisk() error {
	dir := filepath.Dir(s.path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return dberrors.New(dberrors.ErrInternal, "create checkpoint directory", false, err)
		}
	}

	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return dberrors.New(dberrors.ErrInternal, "read checkpoint file", false, err)
	}
	if len(b) == 0 {
		return nil
	}
	if err := json.Unmarshal(b, &s.checkpoints); err != nil {
		return dberrors.New(dberrors.ErrInternal, "decode checkpoint file", false, err)
	}
	s.lastUpdated = time.Now().UTC()
	return nil
}

func (s *FileCheckpointStore) saveToDiskLocked() error {
	b, err := json.MarshalIndent(s.checkpoints, "", "  ")
	if err != nil {
		return dberrors.New(dberrors.ErrInternal, "encode checkpoint file", false, err)
	}

	tmp := s.path + ".tmp"
	if err := s.hooks.writeFile(tmp, b, 0o644); err != nil {
		return dberrors.New(dberrors.ErrInternal, "write temp checkpoint file", false, err)
	}
	if err := s.hooks.rename(tmp, s.path); err != nil {
		return dberrors.New(dberrors.ErrInternal, "replace checkpoint file", false, err)
	}
	return nil
}
