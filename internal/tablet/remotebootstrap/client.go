// Package remotebootstrap implements source-side session management and the
// destination-side client that drives full tablet bootstrap from a remote peer.
//
// Design (docs/plans/plan-remote-bootstrap-and-recovery.md):
//
//	Source side  — Manager (session.go): FetchManifest, FetchFileChunk, FinalizeBootstrap.
//	Destination  — Client (this file): orchestrates session + chunked transfer + install.
package remotebootstrap

import (
	"context"
	"fmt"
	"hash/crc32"
)

// ChunkSize is the default number of bytes requested per FetchFileChunk call.
const ChunkSize = 256 * 1024 // 256 KiB

// Source is the interface the Client uses to communicate with a remote Manager.
// In production this is a thin gRPC stub; in tests it wraps *Manager directly.
type Source interface {
	// StartSession creates a new session on the source for tabletID.
	// Returns a session ID used by subsequent calls.
	StartSession(ctx context.Context, tabletID, localPeerID string) (sessionID string, err error)

	// FetchManifest returns the list of files the destination must download.
	FetchManifest(ctx context.Context, sessionID string) (Manifest, error)

	// FetchFileChunk returns a chunk of file data starting at offset, plus its CRC32.
	FetchFileChunk(ctx context.Context, sessionID, file string, offset int64, size int) (data []byte, crc32c uint32, err error)

	// FinalizeBootstrap releases the checkpoint hold on the source.
	FinalizeBootstrap(ctx context.Context, sessionID string) error

	// AbortSession cancels and cleans up a session on the source.
	AbortSession(ctx context.Context, sessionID string) error
}

// Installer atomically installs the staged bootstrap data for a tablet.
// Implementations may move files from a temp directory into the tablet data directory.
type Installer interface {
	// Install is called after all files have been staged into stagedFiles.
	// stagedFiles maps file name → full file contents.
	// Returns an error if the install fails; the client will NOT call FinalizeBootstrap
	// on the source if Install fails.
	Install(ctx context.Context, tabletID string, stagedFiles map[string][]byte) error
}

// ClientConfig holds tuning parameters for the bootstrap client.
type ClientConfig struct {
	// ChunkSize overrides the default per-fetch chunk size (bytes).
	ChunkSize int
	// LocalPeerID identifies this node to the source.
	LocalPeerID string
}

// Client drives a full remote bootstrap for one tablet.
type Client struct {
	source    Source
	installer Installer
	cfg       ClientConfig
}

// NewClient creates a bootstrap client.
//
//   - source:    interface to the remote bootstrap service.
//   - installer: atomically installs staged files.
//   - cfg:       tuning options.
func NewClient(source Source, installer Installer, cfg ClientConfig) *Client {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = ChunkSize
	}
	return &Client{source: source, installer: installer, cfg: cfg}
}

// Run performs a complete remote bootstrap for tabletID from the source.
//
// Steps (per plan §5b):
//  1. Start session on source.
//  2. Fetch manifest.
//  3. For each file: stream chunks, verify CRC32 per chunk.
//  4. Validate manifest completeness (all files received, sizes match).
//  5. Install atomically via Installer.
//  6. Finalize session on source.
//
// On any error the session is aborted on the source and the staged data
// is discarded.  The caller is responsible for retrying (possibly with a
// different source peer).
func (c *Client) Run(ctx context.Context, tabletID string) error {
	sessionID, err := c.source.StartSession(ctx, tabletID, c.cfg.LocalPeerID)
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}

	manifest, err := c.source.FetchManifest(ctx, sessionID)
	if err != nil {
		_ = c.source.AbortSession(ctx, sessionID)
		return fmt.Errorf("fetch manifest: %w", err)
	}

	staged, err := c.fetchAllFiles(ctx, sessionID, manifest)
	if err != nil {
		_ = c.source.AbortSession(ctx, sessionID)
		return err
	}

	// Validate completeness: every manifest file must be present and correct size.
	for _, fm := range manifest.Files {
		got, ok := staged[fm.Name]
		if !ok {
			_ = c.source.AbortSession(ctx, sessionID)
			return fmt.Errorf("bootstrap: missing staged file %q", fm.Name)
		}
		if len(got) != len(fm.Data) {
			_ = c.source.AbortSession(ctx, sessionID)
			return fmt.Errorf("bootstrap: size mismatch for %q: got %d want %d",
				fm.Name, len(got), len(fm.Data))
		}
	}

	if err := c.installer.Install(ctx, tabletID, staged); err != nil {
		_ = c.source.AbortSession(ctx, sessionID)
		return fmt.Errorf("install: %w", err)
	}

	if err := c.source.FinalizeBootstrap(ctx, sessionID); err != nil {
		// Install succeeded; finalize failure is non-fatal (source cleans up on timeout).
		return fmt.Errorf("finalize (non-fatal): %w", err)
	}
	return nil
}

// fetchAllFiles streams every file in the manifest chunk by chunk,
// verifying the CRC32 of each chunk.  Returns the staged contents.
func (c *Client) fetchAllFiles(ctx context.Context, sessionID string, manifest Manifest) (map[string][]byte, error) {
	staged := make(map[string][]byte, len(manifest.Files))
	for _, fm := range manifest.Files {
		data, err := c.fetchFile(ctx, sessionID, fm.Name, len(fm.Data))
		if err != nil {
			return nil, fmt.Errorf("fetch file %q: %w", fm.Name, err)
		}
		staged[fm.Name] = data
	}
	return staged, nil
}

// fetchFile downloads a single file in chunks, verifying each chunk's CRC32.
func (c *Client) fetchFile(ctx context.Context, sessionID, fileName string, totalSize int) ([]byte, error) {
	result := make([]byte, 0, totalSize)
	offset := int64(0)
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		remaining := totalSize - len(result)
		if remaining <= 0 {
			break
		}
		size := c.cfg.ChunkSize
		if size > remaining {
			size = remaining
		}
		chunk, expectedCRC, err := c.source.FetchFileChunk(ctx, sessionID, fileName, offset, size)
		if err != nil {
			return nil, fmt.Errorf("fetch chunk at offset %d: %w", offset, err)
		}
		if len(chunk) == 0 {
			break // source signals EOF
		}
		actualCRC := crc32.ChecksumIEEE(chunk)
		if actualCRC != expectedCRC {
			return nil, fmt.Errorf("chunk CRC mismatch at offset %d: got %08x want %08x",
				offset, actualCRC, expectedCRC)
		}
		result = append(result, chunk...)
		offset += int64(len(chunk))
	}
	return result, nil
}
