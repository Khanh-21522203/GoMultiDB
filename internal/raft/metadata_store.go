package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Metadata struct {
	CurrentTerm   uint64 `json:"current_term"`
	VotedFor      string `json:"voted_for"`
	ConfigVersion uint64 `json:"config_version"`
}

type MetadataStore interface {
	Load() (Metadata, error)
	Save(meta Metadata) error
}

type FileMetadataStore struct {
	path string
	mu   sync.Mutex
}

func NewFileMetadataStore(dir string) (*FileMetadataStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("metadata dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir metadata dir: %w", err)
	}
	return &FileMetadataStore{path: filepath.Join(dir, "consensus-meta.json")}, nil
}

func (s *FileMetadataStore) Load() (Metadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Metadata{}, nil
		}
		return Metadata{}, fmt.Errorf("read metadata: %w", err)
	}
	var m Metadata
	if err := json.Unmarshal(b, &m); err != nil {
		return Metadata{}, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return m, nil
}

func (s *FileMetadataStore) Save(meta Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("write temp metadata: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace metadata: %w", err)
	}
	return nil
}
