package platform

import (
	"time"

	errors "GoMultiDB/v2/contracts/errors"
)

// FSManager manages the canonical directory layout for a data node.
//
// Layout under each DataDir/WALDir:
//
//	<dir>/data/
//	<dir>/wals/
//	<dir>/tablet-meta/
//	<dir>/consensus-meta/
//	<dir>/snapshots/
//	<dir>/bootstrap_tmp/
type FSManager struct {
	DataDirs []string
	WALDirs  []string
}

// Init creates the expected directory tree under each DataDir and WALDir.
// Returns an error if any directory cannot be created.
//
// Not yet implemented in this scaffold.
func (f *FSManager) Init() error {
	return errors.ErrNotImplemented
}

// TabletDataDir returns the canonical data directory for a tablet.
//
// Not yet implemented in this scaffold.
func (f *FSManager) TabletDataDir(tabletID string) string {
	panic("not implemented")
}

// TabletWALDir returns the canonical WAL directory for a tablet.
//
// Not yet implemented in this scaffold.
func (f *FSManager) TabletWALDir(tabletID string) string {
	panic("not implemented")
}

// TabletMetaPath returns the canonical path for a tablet's meta file.
//
// Not yet implemented in this scaffold.
func (f *FSManager) TabletMetaPath(tabletID string) string {
	panic("not implemented")
}

// NodeInstance is persisted in each data directory to detect data-dir
// swaps.
type NodeInstance struct {
	NodeID     string    `json:"node_id"`
	Generation uint64    `json:"generation"`
	CreatedAt  time.Time `json:"created_at"`
}

// WriteNodeInstance writes (or verifies) a node-instance file in each data
// dir. On first call (file absent), it creates the file; on subsequent
// calls, it reads and verifies NodeID matches, returning an error on
// mismatch.
//
// Not yet implemented in this scaffold.
func (f *FSManager) WriteNodeInstance(nodeID string, generation uint64) error {
	return errors.ErrNotImplemented
}

// ValidateDataDirs checks that each DataDir and WALDir is writable.
//
// Not yet implemented in this scaffold.
func (f *FSManager) ValidateDataDirs() error {
	return errors.ErrNotImplemented
}
