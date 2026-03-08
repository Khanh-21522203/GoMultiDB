package sql

import "testing"

func TestPGProcessBinaryPathSelection(t *testing.T) {
	cfg := PGProcessConfig{
		DataDir:    t.TempDir(),
		BinPath:    "/custom/postgres",
		InitDBPath: "/custom/initdb",
	}
	p, err := NewPGProcess(cfg)
	if err != nil {
		t.Fatalf("NewPGProcess: %v", err)
	}

	if got := p.pgPath(); got != "/custom/postgres" {
		t.Fatalf("pgPath: want custom path, got %s", got)
	}
	if got := p.initDBPath(); got != "/custom/initdb" {
		t.Fatalf("initDBPath: want custom path, got %s", got)
	}
}

func TestCatalogVersionTracker(t *testing.T) {
	cv := NewCatalogVersion()
	if got := cv.Get(); got != 1 {
		t.Fatalf("initial version: want 1, got %d", got)
	}
	if got := cv.Increment(); got != 2 {
		t.Fatalf("incremented version: want 2, got %d", got)
	}
	cv.Set(10)
	if got := cv.Get(); got != 10 {
		t.Fatalf("set version: want 10, got %d", got)
	}
}
