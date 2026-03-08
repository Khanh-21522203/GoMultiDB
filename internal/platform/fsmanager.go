// Package platform provides low-level platform services: filesystem layout,
// memory tracking, and related utilities.
package platform

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
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

var subdirs = []string{"data", "wals", "tablet-meta", "consensus-meta", "snapshots", "bootstrap_tmp"}

// Init creates the expected directory tree under each DataDir and WALDir.
// Returns an error if any directory cannot be created.
func (f *FSManager) Init() error {
	for _, d := range append(f.DataDirs, f.WALDirs...) {
		for _, sub := range subdirs {
			path := filepath.Join(d, sub)
			if err := os.MkdirAll(path, 0o755); err != nil {
				return fmt.Errorf("create dir %s: %w", path, err)
			}
		}
	}
	return nil
}

// TabletDataDir returns the canonical data directory for a tablet.
func (f *FSManager) TabletDataDir(tabletID string) string {
	if len(f.DataDirs) == 0 {
		return filepath.Join("data", tabletID)
	}
	return filepath.Join(f.DataDirs[0], "data", tabletID)
}

// TabletWALDir returns the canonical WAL directory for a tablet.
func (f *FSManager) TabletWALDir(tabletID string) string {
	if len(f.WALDirs) == 0 {
		return filepath.Join("wals", tabletID)
	}
	return filepath.Join(f.WALDirs[0], "wals", tabletID)
}

// TabletMetaPath returns the canonical path for a tablet's meta file.
func (f *FSManager) TabletMetaPath(tabletID string) string {
	if len(f.DataDirs) == 0 {
		return filepath.Join("tablet-meta", tabletID+".meta")
	}
	return filepath.Join(f.DataDirs[0], "tablet-meta", tabletID+".meta")
}

// ── NodeInstanceFile ──────────────────────────────────────────────────────────

// NodeInstance is persisted in each data directory to detect data-dir swaps.
type NodeInstance struct {
	NodeID     string    `json:"node_id"`
	Generation uint64    `json:"generation"`
	CreatedAt  time.Time `json:"created_at"`
}

// WriteNodeInstance writes (or verifies) a node-instance file in each data dir.
// On first call (file absent): creates the file.
// On subsequent calls: reads and verifies NodeID matches. Returns error on mismatch.
func (f *FSManager) WriteNodeInstance(nodeID string, generation uint64) error {
	inst := NodeInstance{
		NodeID:     nodeID,
		Generation: generation,
		CreatedAt:  time.Now().UTC(),
	}
	for _, d := range f.DataDirs {
		path := filepath.Join(d, "node-instance.json")
		existing, err := readNodeInstance(path)
		if err == nil {
			// File exists — verify NodeID.
			if existing.NodeID != nodeID {
				return fmt.Errorf("node instance mismatch in %s: stored=%s current=%s",
					d, existing.NodeID, nodeID)
			}
			continue
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("read node instance %s: %w", path, err)
		}
		// File absent — write it.
		b, err := json.Marshal(inst)
		if err != nil {
			return fmt.Errorf("marshal node instance: %w", err)
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return fmt.Errorf("write node instance %s: %w", path, err)
		}
	}
	return nil
}

func readNodeInstance(path string) (NodeInstance, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return NodeInstance{}, err
	}
	var inst NodeInstance
	if err := json.Unmarshal(b, &inst); err != nil {
		return NodeInstance{}, err
	}
	return inst, nil
}

// ValidateDataDirs checks that each DataDir and WALDir is writable.
func (f *FSManager) ValidateDataDirs() error {
	for _, d := range append(f.DataDirs, f.WALDirs...) {
		probe := filepath.Join(d, ".writable_probe")
		if err := os.WriteFile(probe, []byte("probe"), 0o644); err != nil {
			return fmt.Errorf("data dir %s is not writable: %w", d, err)
		}
		_ = os.Remove(probe)
	}
	return nil
}
