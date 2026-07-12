package tablet

import (
	errors "GoMultiDB/v2/contracts/errors"
)

// MetaStore is the durability contract for tablet lifecycle markers.
//
// WriteMeta MUST be called before the corresponding in-memory state
// transition. If the process crashes after WriteMeta but before the
// in-memory update, the next startup will recover from the on-disk marker
// and apply recovery rules.
type MetaStore interface {
	// WriteMeta durably writes the tablet meta. Implementations must fsync
	// before returning.
	WriteMeta(m Meta) error
	// DeleteMeta removes the tablet meta file (used on hard delete).
	DeleteMeta(tabletID string) error
	// LoadAll returns all persisted tablet metas. Called once at startup.
	LoadAll() ([]Meta, error)
}

// NoopMetaStore satisfies MetaStore with no I/O. Used for in-memory-only
// mode (tests).
type NoopMetaStore struct{}

var _ MetaStore = NoopMetaStore{}

// WriteMeta is a no-op.
func (NoopMetaStore) WriteMeta(m Meta) error { return nil }

// DeleteMeta is a no-op.
func (NoopMetaStore) DeleteMeta(tabletID string) error { return nil }

// LoadAll returns no metas.
func (NoopMetaStore) LoadAll() ([]Meta, error) { return nil, nil }

// FileMetaStore persists tablet metadata to disk using atomic rename +
// fsync.
//
// Each tablet is stored as <dir>/<tablet-id>.meta containing JSON-encoded
// Meta. The write path uses a temp file + rename to ensure the file is
// never partially written.
//
// Scaffold stub: WriteMeta, DeleteMeta, and LoadAll return
// errors.ErrNotImplemented.
type FileMetaStore struct {
	dir string
}

var _ MetaStore = (*FileMetaStore)(nil)

// NewFileMetaStore creates a FileMetaStore rooted at dir, creating it if
// necessary.
func NewFileMetaStore(dir string) (*FileMetaStore, error) {
	return &FileMetaStore{}, nil
}

// WriteMeta atomically writes m to disk and fsyncs before returning. The
// caller must ensure m.TabletID is non-empty.
//
// Not yet implemented in this scaffold.
func (s *FileMetaStore) WriteMeta(m Meta) error {
	return errors.ErrNotImplemented
}

// DeleteMeta removes the marker file for tabletID. Not-exist is not an
// error.
//
// Not yet implemented in this scaffold.
func (s *FileMetaStore) DeleteMeta(tabletID string) error {
	return errors.ErrNotImplemented
}

// LoadAll reads all *.meta files in the directory and returns their
// decoded contents.
//
// Not yet implemented in this scaffold.
func (s *FileMetaStore) LoadAll() ([]Meta, error) {
	return nil, errors.ErrNotImplemented
}
