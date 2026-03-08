package remotebootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/tablet/remotebootstrap"
)

// ── in-process Source adapter ─────────────────────────────────────────────────

// localSource wraps a *remotebootstrap.Manager as a Source, adding StartSession.
type localSource struct {
	m *remotebootstrap.Manager
	// manifest is the snapshot the source will serve; set before Run.
	manifest remotebootstrap.Manifest
	// interceptFetch, if set, is called instead of the real FetchFileChunk.
	interceptFetch func(ctx context.Context, sessionID, file string, offset int64, size int) ([]byte, uint32, error)
}

func (s *localSource) StartSession(_ context.Context, tabletID, _ string) (string, error) {
	s.manifest.TabletID = tabletID
	return s.m.StartRemoteBootstrap(tabletID, "source-peer", s.manifest)
}

func (s *localSource) FetchManifest(_ context.Context, sessionID string) (remotebootstrap.Manifest, error) {
	return s.m.FetchManifest(sessionID)
}

func (s *localSource) FetchFileChunk(ctx context.Context, sessionID, file string, offset int64, size int) ([]byte, uint32, error) {
	if s.interceptFetch != nil {
		return s.interceptFetch(ctx, sessionID, file, offset, size)
	}
	return s.m.FetchFileChunk(sessionID, file, offset, size)
}

func (s *localSource) FinalizeBootstrap(_ context.Context, sessionID string) error {
	return s.m.FinalizeBootstrap(sessionID)
}

func (s *localSource) AbortSession(_ context.Context, sessionID string) error {
	return s.m.AbortSession(sessionID)
}

// ── simple installer ──────────────────────────────────────────────────────────

type recordingInstaller struct {
	installed map[string][]byte
	err       error
}

func newRecordingInstaller() *recordingInstaller {
	return &recordingInstaller{installed: make(map[string][]byte)}
}

func (r *recordingInstaller) Install(_ context.Context, _ string, files map[string][]byte) error {
	if r.err != nil {
		return r.err
	}
	for k, v := range files {
		r.installed[k] = append([]byte(nil), v...)
	}
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newLocalSource(m *remotebootstrap.Manager, files ...remotebootstrap.FileMeta) *localSource {
	return &localSource{m: m, manifest: remotebootstrap.Manifest{Files: files}}
}

func buildClient(src remotebootstrap.Source, inst remotebootstrap.Installer, chunkSize int) *remotebootstrap.Client {
	return remotebootstrap.NewClient(src, inst, remotebootstrap.ClientConfig{
		ChunkSize:   chunkSize,
		LocalPeerID: "dest-peer",
	})
}

// ── happy path ────────────────────────────────────────────────────────────────

func TestClientHappyPath(t *testing.T) {
	m := remotebootstrap.NewManager()
	src := newLocalSource(m,
		remotebootstrap.FileMeta{Name: "regular.sst", Data: []byte("hello world")},
		remotebootstrap.FileMeta{Name: "wal.seg", Data: []byte("12345")},
	)
	inst := newRecordingInstaller()
	client := buildClient(src, inst, 4) // small chunks to exercise chunking

	if err := client.Run(context.Background(), "tablet-1"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, fm := range src.manifest.Files {
		got, ok := inst.installed[fm.Name]
		if !ok {
			t.Fatalf("file %q not installed", fm.Name)
		}
		if string(got) != string(fm.Data) {
			t.Fatalf("file %q: got %q want %q", fm.Name, got, fm.Data)
		}
	}
}

func TestClientEmptyManifest(t *testing.T) {
	m := remotebootstrap.NewManager()
	src := newLocalSource(m) // no files
	inst := newRecordingInstaller()
	client := buildClient(src, inst, 1024)

	if err := client.Run(context.Background(), "tablet-empty"); err != nil {
		t.Fatalf("empty manifest should succeed: %v", err)
	}
	if len(inst.installed) != 0 {
		t.Fatalf("expected no installed files, got %d", len(inst.installed))
	}
}

// ── chunk CRC mismatch ────────────────────────────────────────────────────────

func TestClientChunkCRCMismatch(t *testing.T) {
	m := remotebootstrap.NewManager()
	src := newLocalSource(m,
		remotebootstrap.FileMeta{Name: "data.sst", Data: []byte("abcdef")},
	)
	// Intercept to corrupt the CRC.
	src.interceptFetch = func(ctx context.Context, sessionID, file string, offset int64, size int) ([]byte, uint32, error) {
		data, _, err := m.FetchFileChunk(sessionID, file, offset, size)
		return data, 0xDEADBEEF, err // wrong CRC
	}
	inst := newRecordingInstaller()
	client := buildClient(src, inst, 1024)

	err := client.Run(context.Background(), "tablet-crc")
	if err == nil {
		t.Fatalf("expected CRC mismatch error")
	}
	if !errors.Is(err, err) { // just verify non-nil
		t.Fatalf("unexpected error type: %T", err)
	}
	// Installer must NOT have been called.
	if len(inst.installed) != 0 {
		t.Fatalf("installer should not be called after CRC mismatch")
	}
}

// ── install failure ───────────────────────────────────────────────────────────

func TestClientInstallFailure(t *testing.T) {
	m := remotebootstrap.NewManager()
	src := newLocalSource(m,
		remotebootstrap.FileMeta{Name: "f.sst", Data: []byte("data")},
	)
	inst := newRecordingInstaller()
	inst.err = errors.New("disk full")
	client := buildClient(src, inst, 1024)

	if err := client.Run(context.Background(), "tablet-install-fail"); err == nil {
		t.Fatalf("expected install error")
	}
}

// ── context cancellation ──────────────────────────────────────────────────────

func TestClientContextCancelled(t *testing.T) {
	m := remotebootstrap.NewManager()
	// Large file so chunking takes multiple iterations.
	bigData := make([]byte, 1024*1024)
	for i := range bigData {
		bigData[i] = byte(i)
	}
	src := newLocalSource(m,
		remotebootstrap.FileMeta{Name: "big.sst", Data: bigData},
	)
	callCount := 0
	src.interceptFetch = func(ctx context.Context, sessionID, file string, offset int64, size int) ([]byte, uint32, error) {
		callCount++
		if callCount >= 2 {
			// Simulate context cancel after first chunk.
			return nil, 0, context.Canceled
		}
		return m.FetchFileChunk(sessionID, file, offset, size)
	}
	inst := newRecordingInstaller()
	client := buildClient(src, inst, 4096)

	err := client.Run(context.Background(), "tablet-ctx")
	if err == nil {
		t.Fatalf("expected error after context cancellation")
	}
}

// ── concurrent session cap ────────────────────────────────────────────────────

func TestConcurrentSessionCap(t *testing.T) {
	m := remotebootstrap.NewManagerWithConfig(remotebootstrap.ManagerConfig{
		MaxConcurrentSessions: 2,
		SessionTimeout:        time.Hour,
	})

	// Occupy both slots.
	id1, err := m.StartRemoteBootstrap("t1", "peer", remotebootstrap.Manifest{})
	if err != nil {
		t.Fatalf("session 1: %v", err)
	}
	_, err = m.StartRemoteBootstrap("t2", "peer", remotebootstrap.Manifest{})
	if err != nil {
		t.Fatalf("session 2: %v", err)
	}

	// Third should be rejected.
	_, err = m.StartRemoteBootstrap("t3", "peer", remotebootstrap.Manifest{})
	if err == nil {
		t.Fatalf("expected cap error for 3rd session")
	}
	var dbe dberrors.DBError
	if !errors.As(err, &dbe) || dbe.Code != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected ErrRetryableUnavailable, got: %v", err)
	}

	// After aborting session 1, a new session should succeed.
	if err := m.AbortSession(id1); err != nil {
		t.Fatalf("abort: %v", err)
	}
	_, err = m.StartRemoteBootstrap("t4", "peer", remotebootstrap.Manifest{})
	if err != nil {
		t.Fatalf("session 4 after abort: %v", err)
	}
}

// ── session timeout ───────────────────────────────────────────────────────────

func TestSessionTimeout(t *testing.T) {
	now := time.Now().UTC()
	m := remotebootstrap.NewManagerWithConfig(remotebootstrap.ManagerConfig{
		MaxConcurrentSessions: 4,
		SessionTimeout:        5 * time.Minute,
		NowFn:                 func() time.Time { return now },
	})

	for i := 0; i < 3; i++ {
		_, err := m.StartRemoteBootstrap(fmt.Sprintf("t%d", i), "peer", remotebootstrap.Manifest{})
		if err != nil {
			t.Fatalf("start session %d: %v", i, err)
		}
	}
	if got := m.ActiveCount(); got != 3 {
		t.Fatalf("expected 3 active, got %d", got)
	}

	// Advance time past timeout.
	now = now.Add(6 * time.Minute)
	removed := m.ExpireStaleSessions()
	if removed != 3 {
		t.Fatalf("expected 3 expired, got %d", removed)
	}
	if got := m.ActiveCount(); got != 0 {
		t.Fatalf("expected 0 active after expiry, got %d", got)
	}
}

// ── abort cleans up ───────────────────────────────────────────────────────────

func TestAbortSessionRemovesSession(t *testing.T) {
	m := remotebootstrap.NewManager()
	id, err := m.StartRemoteBootstrap("tablet-abort", "peer", remotebootstrap.Manifest{})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := m.ActiveCount(); got != 1 {
		t.Fatalf("expected 1 active, got %d", got)
	}
	if err := m.AbortSession(id); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if got := m.ActiveCount(); got != 0 {
		t.Fatalf("expected 0 active after abort, got %d", got)
	}
	// Second abort returns error (not found).
	if err := m.AbortSession(id); err == nil {
		t.Fatalf("expected error on double abort")
	}
}
