package remotebootstrap

import (
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

type SessionState string

const (
	SessionInit       SessionState = "INIT"
	SessionTransferring SessionState = "TRANSFERRING"
	SessionFinalized  SessionState = "FINALIZED"
	SessionFailed     SessionState = "FAILED"
)

type FileMeta struct {
	Name string
	Data []byte
}

type Manifest struct {
	TabletID string
	Files    []FileMeta
}

type Session struct {
	SessionID    string
	TabletID     string
	SourcePeerID string
	StartedAt    time.Time
	State        SessionState
	Manifest     Manifest
	Staged       map[string][]byte
}

type Manager struct {
	mu       sync.Mutex
	sessions map[string]*Session
	nextID   uint64
}

func NewManager() *Manager {
	return &Manager{sessions: make(map[string]*Session)}
}

func (m *Manager) StartRemoteBootstrap(tabletID, sourcePeer string, manifest Manifest) (string, error) {
	if tabletID == "" || sourcePeer == "" {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "tabletID and sourcePeer are required", false, nil)
	}
	if manifest.TabletID != "" && manifest.TabletID != tabletID {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "manifest tabletID mismatch", false, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	sessionID := fmt.Sprintf("rb-%d", m.nextID)
	s := &Session{
		SessionID:    sessionID,
		TabletID:     tabletID,
		SourcePeerID: sourcePeer,
		StartedAt:    time.Now().UTC(),
		State:        SessionTransferring,
		Manifest:     manifest,
		Staged:       make(map[string][]byte),
	}
	s.Manifest.TabletID = tabletID
	m.sessions[sessionID] = s
	return sessionID, nil
}

func (m *Manager) FetchManifest(sessionID string) (Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return Manifest{}, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	return s.Manifest, nil
}

func (m *Manager) FetchFileChunk(sessionID, file string, offset int64, size int) ([]byte, uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return nil, 0, dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.State != SessionTransferring {
		return nil, 0, dberrors.New(dberrors.ErrConflict, "session is not transferable", true, nil)
	}
	for _, f := range s.Manifest.Files {
		if f.Name != file {
			continue
		}
		if offset < 0 || offset > int64(len(f.Data)) {
			return nil, 0, dberrors.New(dberrors.ErrInvalidArgument, "invalid offset", false, nil)
		}
		end := int(offset) + size
		if size <= 0 || end > len(f.Data) {
			end = len(f.Data)
		}
		chunk := append([]byte(nil), f.Data[offset:end]...)
		return chunk, crc32.ChecksumIEEE(chunk), nil
	}
	return nil, 0, dberrors.New(dberrors.ErrInvalidArgument, "file not found", false, nil)
}

func (m *Manager) StageFileChunk(sessionID, file string, chunk []byte, expectedCRC uint32) error {
	if crc32.ChecksumIEEE(chunk) != expectedCRC {
		return dberrors.New(dberrors.ErrConflict, "chunk checksum mismatch", true, nil)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	s.Staged[file] = append(s.Staged[file], chunk...)
	return nil
}

func (m *Manager) FinalizeBootstrap(sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	if s.State != SessionTransferring {
		return dberrors.New(dberrors.ErrConflict, "session cannot be finalized", true, nil)
	}
	for _, f := range s.Manifest.Files {
		staged, ok := s.Staged[f.Name]
		if !ok {
			s.State = SessionFailed
			return dberrors.New(dberrors.ErrConflict, "missing staged file", true, nil)
		}
		if len(staged) != len(f.Data) || crc32.ChecksumIEEE(staged) != crc32.ChecksumIEEE(f.Data) {
			s.State = SessionFailed
			return dberrors.New(dberrors.ErrConflict, "staged file validation failed", true, nil)
		}
	}
	s.State = SessionFinalized
	return nil
}
