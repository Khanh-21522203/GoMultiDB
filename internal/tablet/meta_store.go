package tablet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MetaStore is the durability contract for tablet lifecycle markers.
//
// WriteMeta MUST be called before the corresponding in-memory state transition.
// If the process crashes after WriteMeta but before the in-memory update, the
// next startup will recover from the on-disk marker and apply recovery rules.
type MetaStore interface {
	// WriteMeta durably writes the tablet meta. Implementations must fsync before returning.
	WriteMeta(m Meta) error
	// DeleteMeta removes the tablet meta file (used on hard delete).
	DeleteMeta(tabletID string) error
	// LoadAll returns all persisted tablet metas. Called once at startup.
	LoadAll() ([]Meta, error)
}

// NoopMetaStore satisfies MetaStore with no I/O. Used for in-memory-only mode (tests).
type NoopMetaStore struct{}

func (NoopMetaStore) WriteMeta(_ Meta) error    { return nil }
func (NoopMetaStore) DeleteMeta(_ string) error { return nil }
func (NoopMetaStore) LoadAll() ([]Meta, error)  { return nil, nil }

// FileMetaStore persists tablet metadata to disk using atomic rename + fsync.
//
// Each tablet is stored as <dir>/<tablet-id>.meta containing JSON-encoded Meta.
// The write path uses a temp file + rename to ensure the file is never partially written.
type FileMetaStore struct {
	dir string
}

// NewFileMetaStore creates a FileMetaStore rooted at dir, creating it if necessary.
func NewFileMetaStore(dir string) (*FileMetaStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("tablet meta store: create dir %q: %w", dir, err)
	}
	return &FileMetaStore{dir: dir}, nil
}

func (s *FileMetaStore) metaPath(tabletID string) string {
	return filepath.Join(s.dir, tabletID+".meta")
}

// WriteMeta atomically writes m to disk and fsyncs before returning.
// The caller must ensure m.TabletID is non-empty.
func (s *FileMetaStore) WriteMeta(m Meta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("tablet meta store: marshal %q: %w", m.TabletID, err)
	}
	path := s.metaPath(m.TabletID)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("tablet meta store: open tmp %q: %w", tmp, err)
	}
	if _, werr := f.Write(data); werr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("tablet meta store: write %q: %w", tmp, werr)
	}
	if serr := f.Sync(); serr != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("tablet meta store: fsync %q: %w", tmp, serr)
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("tablet meta store: close %q: %w", tmp, cerr)
	}
	if rerr := os.Rename(tmp, path); rerr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("tablet meta store: rename %q -> %q: %w", tmp, path, rerr)
	}
	return nil
}

// DeleteMeta removes the marker file for tabletID. Not-exist is not an error.
func (s *FileMetaStore) DeleteMeta(tabletID string) error {
	err := os.Remove(s.metaPath(tabletID))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("tablet meta store: delete %q: %w", tabletID, err)
	}
	return nil
}

// LoadAll reads all *.meta files in the directory and returns their decoded contents.
func (s *FileMetaStore) LoadAll() ([]Meta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("tablet meta store: read dir %q: %w", s.dir, err)
	}
	var metas []Meta
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".meta") {
			continue
		}
		path := filepath.Join(s.dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("tablet meta store: read %q: %w", path, err)
		}
		var m Meta
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, fmt.Errorf("tablet meta store: unmarshal %q: %w", path, err)
		}
		metas = append(metas, m)
	}
	return metas, nil
}
