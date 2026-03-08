package platform

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFSManagerInitCreatesSubdirs(t *testing.T) {
	base := t.TempDir()
	walBase := t.TempDir()

	fm := &FSManager{
		DataDirs: []string{base},
		WALDirs:  []string{walBase},
	}
	if err := fm.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	for _, dir := range []string{base, walBase} {
		for _, sub := range subdirs {
			path := filepath.Join(dir, sub)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("expected subdir %s to exist: %v", path, err)
			}
			if !info.IsDir() {
				t.Fatalf("expected %s to be a directory", path)
			}
		}
	}
}

func TestFSManagerInitIdempotent(t *testing.T) {
	base := t.TempDir()
	fm := &FSManager{DataDirs: []string{base}}
	if err := fm.Init(); err != nil {
		t.Fatalf("first Init: %v", err)
	}
	if err := fm.Init(); err != nil {
		t.Fatalf("second Init (idempotent): %v", err)
	}
}

func TestFSManagerTabletDataDir(t *testing.T) {
	base := t.TempDir()
	fm := &FSManager{DataDirs: []string{base}}
	got := fm.TabletDataDir("tab-1")
	want := filepath.Join(base, "data", "tab-1")
	if got != want {
		t.Fatalf("TabletDataDir: want %s, got %s", want, got)
	}
}

func TestFSManagerTabletDataDirFallback(t *testing.T) {
	fm := &FSManager{}
	got := fm.TabletDataDir("tab-x")
	want := filepath.Join("data", "tab-x")
	if got != want {
		t.Fatalf("TabletDataDir (no dirs): want %s, got %s", want, got)
	}
}

func TestFSManagerTabletWALDir(t *testing.T) {
	walBase := t.TempDir()
	fm := &FSManager{WALDirs: []string{walBase}}
	got := fm.TabletWALDir("tab-1")
	want := filepath.Join(walBase, "wals", "tab-1")
	if got != want {
		t.Fatalf("TabletWALDir: want %s, got %s", want, got)
	}
}

func TestFSManagerTabletMetaPath(t *testing.T) {
	base := t.TempDir()
	fm := &FSManager{DataDirs: []string{base}}
	got := fm.TabletMetaPath("tab-1")
	want := filepath.Join(base, "tablet-meta", "tab-1.meta")
	if got != want {
		t.Fatalf("TabletMetaPath: want %s, got %s", want, got)
	}
}

func TestFSManagerValidateDataDirs(t *testing.T) {
	base := t.TempDir()
	fm := &FSManager{DataDirs: []string{base}}
	if err := fm.ValidateDataDirs(); err != nil {
		t.Fatalf("ValidateDataDirs on writable dir: %v", err)
	}
}

func TestFSManagerWriteNodeInstanceFirstWrite(t *testing.T) {
	base := t.TempDir()
	fm := &FSManager{DataDirs: []string{base}}
	if err := fm.WriteNodeInstance("node-abc", 1); err != nil {
		t.Fatalf("WriteNodeInstance: %v", err)
	}
	path := filepath.Join(base, "node-instance.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("node-instance.json not created: %v", err)
	}
}

func TestFSManagerWriteNodeInstanceIdempotent(t *testing.T) {
	base := t.TempDir()
	fm := &FSManager{DataDirs: []string{base}}
	if err := fm.WriteNodeInstance("node-abc", 1); err != nil {
		t.Fatalf("first write: %v", err)
	}
	// same nodeID — must succeed
	if err := fm.WriteNodeInstance("node-abc", 2); err != nil {
		t.Fatalf("second write same node: %v", err)
	}
}

func TestFSManagerWriteNodeInstanceMismatch(t *testing.T) {
	base := t.TempDir()
	fm := &FSManager{DataDirs: []string{base}}
	if err := fm.WriteNodeInstance("node-abc", 1); err != nil {
		t.Fatalf("first write: %v", err)
	}
	// different nodeID — must fail (data dir swap detection)
	err := fm.WriteNodeInstance("node-xyz", 1)
	if err == nil {
		t.Fatalf("expected mismatch error when node ID changes")
	}
}
