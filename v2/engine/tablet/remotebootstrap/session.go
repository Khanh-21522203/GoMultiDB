package remotebootstrap

import (
	"sync"
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// DefaultMaxConcurrentSessions is the maximum number of simultaneous
// bootstrap sessions a single source node will serve.
const DefaultMaxConcurrentSessions = 4

// DefaultSessionTimeout is how long a session may be idle before it is
// automatically cleaned up.
const DefaultSessionTimeout = 10 * time.Minute

// SessionState is a source-side bootstrap session's lifecycle state.
type SessionState string

// Bootstrap session states.
const (
	SessionInit         SessionState = "INIT"
	SessionTransferring SessionState = "TRANSFERRING"
	SessionFinalized    SessionState = "FINALIZED"
	SessionFailed       SessionState = "FAILED"
	SessionAborted      SessionState = "ABORTED"
)

// FileMeta identifies a single file within a Manifest and its contents.
type FileMeta struct {
	Name string
	Data []byte
}

// Manifest lists the files a destination must download to bootstrap a
// tablet.
type Manifest struct {
	TabletID string
	Files    []FileMeta
}

// Session is the source-side state for one in-progress bootstrap.
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

// Manager is the source-side bootstrap session manager.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented or panic.
type Manager struct {
	mu       sync.Mutex
	sessions map[string]*Session
	nextID   uint64
	cfg      ManagerConfig
}

// NewManager returns a Manager with default configuration.
func NewManager() *Manager {
	return &Manager{}
}

// NewManagerWithConfig returns a Manager using cfg, defaulting any
// zero-valued fields.
func NewManagerWithConfig(cfg ManagerConfig) *Manager {
	return &Manager{}
}

// ActiveCount returns the number of sessions currently in the
// TRANSFERRING state.
//
// Not yet implemented in this scaffold.
func (m *Manager) ActiveCount() int {
	panic("not implemented")
}

// AbortSession marks a session as aborted and removes it from the
// registry.
//
// Not yet implemented in this scaffold.
func (m *Manager) AbortSession(sessionID string) error {
	return errors.ErrNotImplemented
}

// ExpireStaleSessions removes sessions that have been idle longer than
// SessionTimeout. Returns the number of sessions removed.
//
// Not yet implemented in this scaffold.
func (m *Manager) ExpireStaleSessions() int {
	panic("not implemented")
}

// StartRemoteBootstrap creates a new source-side session for tabletID,
// seeded with manifest, subject to the concurrent-session cap.
//
// Not yet implemented in this scaffold.
func (m *Manager) StartRemoteBootstrap(tabletID, sourcePeer string, manifest Manifest) (string, error) {
	return "", errors.ErrNotImplemented
}

// FetchManifest returns the manifest for sessionID.
//
// Not yet implemented in this scaffold.
func (m *Manager) FetchManifest(sessionID string) (Manifest, error) {
	return Manifest{}, errors.ErrNotImplemented
}

// FetchFileChunk returns up to size bytes of file starting at offset,
// along with their CRC32.
//
// Not yet implemented in this scaffold.
func (m *Manager) FetchFileChunk(sessionID, file string, offset int64, size int) ([]byte, uint32, error) {
	return nil, 0, errors.ErrNotImplemented
}

// StageFileChunk appends chunk to file's staged contents for sessionID,
// after verifying it against expectedCRC.
//
// Not yet implemented in this scaffold.
func (m *Manager) StageFileChunk(sessionID, file string, chunk []byte, expectedCRC uint32) error {
	return errors.ErrNotImplemented
}

// FinalizeBootstrap marks the session as finalized and removes it from the
// registry, releasing any checkpoint hold held by the source.
//
// In the split source/destination architecture, completeness validation is
// performed on the destination side (by Client) before Install is called.
// The source-side finalize is simply a cleanup notification.
//
// Not yet implemented in this scaffold.
func (m *Manager) FinalizeBootstrap(sessionID string) error {
	return errors.ErrNotImplemented
}

// FinalizeWithValidation is the legacy in-process finalize that validates
// the staged files (uploaded via StageFileChunk) before marking the
// session done. Used by direct in-process callers that exercise the
// StageFileChunk path.
//
// Not yet implemented in this scaffold.
func (m *Manager) FinalizeWithValidation(sessionID string) error {
	return errors.ErrNotImplemented
}
