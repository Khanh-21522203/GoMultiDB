package remotebootstrap

import (
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
)

// DefaultMaxConcurrentSessions is the maximum number of simultaneous bootstrap
// sessions a single source node will serve.
const DefaultMaxConcurrentSessions = 4

// DefaultSessionTimeout is how long a session may be idle before it is
// automatically cleaned up.
const DefaultSessionTimeout = 10 * time.Minute

type SessionState string

const (
	SessionInit         SessionState = "INIT"
	SessionTransferring SessionState = "TRANSFERRING"
	SessionFinalized    SessionState = "FINALIZED"
	SessionFailed       SessionState = "FAILED"
	SessionAborted      SessionState = "ABORTED"
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

// ManagerConfig configures the session manager.
type ManagerConfig struct {
	// MaxConcurrentSessions caps simultaneous bootstrap sessions.
	// Zero → DefaultMaxConcurrentSessions.
	MaxConcurrentSessions int
	// SessionTimeout is the idle timeout before auto-cleanup.
	// Zero → DefaultSessionTimeout.
	SessionTimeout time.Duration
	// NowFn returns the current time (injectable for tests).
	NowFn func() time.Time
}

type Manager struct {
	mu      sync.Mutex
	sessions map[string]*Session
	nextID  uint64
	cfg     ManagerConfig
}

func NewManager() *Manager {
	return NewManagerWithConfig(ManagerConfig{})
}

func NewManagerWithConfig(cfg ManagerConfig) *Manager {
	if cfg.MaxConcurrentSessions <= 0 {
		cfg.MaxConcurrentSessions = DefaultMaxConcurrentSessions
	}
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = DefaultSessionTimeout
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() time.Time { return time.Now().UTC() }
	}
	return &Manager{sessions: make(map[string]*Session), cfg: cfg}
}

// ActiveCount returns the number of sessions currently in the TRANSFERRING state.
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, s := range m.sessions {
		if s.State == SessionTransferring {
			count++
		}
	}
	return count
}

// AbortSession marks a session as aborted and removes it from the registry.
func (m *Manager) AbortSession(sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "session not found", false, nil)
	}
	s.State = SessionAborted
	delete(m.sessions, sessionID)
	return nil
}

// ExpireStaleSessions removes sessions that have been idle longer than SessionTimeout.
// Returns the number of sessions removed.
func (m *Manager) ExpireStaleSessions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.cfg.NowFn()
	removed := 0
	for id, s := range m.sessions {
		if s.State == SessionTransferring && now.Sub(s.StartedAt) > m.cfg.SessionTimeout {
			s.State = SessionFailed
			delete(m.sessions, id)
			removed++
		}
	}
	return removed
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

	// Enforce concurrent session cap.
	active := 0
	for _, s := range m.sessions {
		if s.State == SessionTransferring {
			active++
		}
	}
	if active >= m.cfg.MaxConcurrentSessions {
		return "", dberrors.New(dberrors.ErrRetryableUnavailable,
			"max concurrent bootstrap sessions reached", true, nil)
	}

	m.nextID++
	sessionID := fmt.Sprintf("rb-%d", m.nextID)
	s := &Session{
		SessionID:    sessionID,
		TabletID:     tabletID,
		SourcePeerID: sourcePeer,
		StartedAt:    m.cfg.NowFn(),
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

// FinalizeBootstrap marks the session as finalized and removes it from the
// registry, releasing any checkpoint hold held by the source.
//
// In the split source/destination architecture, completeness validation is
// performed on the destination side (by the Client) before Install is called.
// The source-side finalize is simply a cleanup notification.
//
// When the Manager is used in the legacy in-process mode (StageFileChunk path),
// FinalizeWithValidation can be used instead.
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
	s.State = SessionFinalized
	delete(m.sessions, sessionID)
	return nil
}

// FinalizeWithValidation is the legacy in-process finalize that validates the
// staged files (uploaded via StageFileChunk) before marking the session done.
// Used by direct in-process tests that exercise the StageFileChunk path.
func (m *Manager) FinalizeWithValidation(sessionID string) error {
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
	delete(m.sessions, sessionID)
	return nil
}
