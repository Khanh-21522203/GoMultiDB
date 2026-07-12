package remotebootstrap

import (
	"context"

	errors "GoMultiDB/v2/contracts/errors"
)

// ChunkSize is the default number of bytes requested per FetchFileChunk
// call.
const ChunkSize = 256 * 1024 // 256 KiB

// Source is the interface the Client uses to communicate with a remote
// Manager. In production this is a thin RPC stub; in tests it wraps
// *Manager directly.
type Source interface {
	// StartSession creates a new session on the source for tabletID.
	// Returns a session ID used by subsequent calls.
	StartSession(ctx context.Context, tabletID, localPeerID string) (sessionID string, err error)

	// FetchManifest returns the list of files the destination must
	// download.
	FetchManifest(ctx context.Context, sessionID string) (Manifest, error)

	// FetchFileChunk returns a chunk of file data starting at offset, plus
	// its CRC32.
	FetchFileChunk(ctx context.Context, sessionID, file string, offset int64, size int) (data []byte, crc32c uint32, err error)

	// FinalizeBootstrap releases the checkpoint hold on the source.
	FinalizeBootstrap(ctx context.Context, sessionID string) error

	// AbortSession cancels and cleans up a session on the source.
	AbortSession(ctx context.Context, sessionID string) error
}

// Installer atomically installs the staged bootstrap data for a tablet.
// Implementations may move files from a temp directory into the tablet
// data directory.
type Installer interface {
	// Install is called after all files have been staged into
	// stagedFiles. stagedFiles maps file name → full file contents.
	// Returns an error if the install fails; the client will NOT call
	// FinalizeBootstrap on the source if Install fails.
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
//
// Scaffold stub: Run returns errors.ErrNotImplemented.
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
	return &Client{}
}

// Run performs a complete remote bootstrap for tabletID from the source:
// start a session, fetch the manifest, stream and verify every file's
// chunks, validate manifest completeness, install atomically via
// Installer, and finalize the session on the source. On any error the
// session is aborted on the source and staged data is discarded; the
// caller is responsible for retrying, possibly with a different source
// peer.
//
// Not yet implemented in this scaffold.
func (c *Client) Run(ctx context.Context, tabletID string) error {
	return errors.ErrNotImplemented
}
