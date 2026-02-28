package remotebootstrap_test

import (
	"testing"

	"GoMultiDB/internal/tablet/remotebootstrap"
)

func TestRemoteBootstrapSessionHappyPath(t *testing.T) {
	m := remotebootstrap.NewManager()
	manifest := remotebootstrap.Manifest{
		Files: []remotebootstrap.FileMeta{
			{Name: "regular.sst", Data: []byte("abcdef")},
			{Name: "wal.seg", Data: []byte("12345678")},
		},
	}
	sessionID, err := m.StartRemoteBootstrap("tablet-1", "peer-a", manifest)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	mf, err := m.FetchManifest(sessionID)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	for _, f := range mf.Files {
		offset := int64(0)
		for offset < int64(len(f.Data)) {
			chunk, crc, err := m.FetchFileChunk(sessionID, f.Name, offset, 3)
			if err != nil {
				t.Fatalf("fetch chunk: %v", err)
			}
			if err := m.StageFileChunk(sessionID, f.Name, chunk, crc); err != nil {
				t.Fatalf("stage chunk: %v", err)
			}
			offset += int64(len(chunk))
		}
	}
	if err := m.FinalizeBootstrap(sessionID); err != nil {
		t.Fatalf("finalize: %v", err)
	}
}

func TestRemoteBootstrapChecksumMismatch(t *testing.T) {
	m := remotebootstrap.NewManager()
	sessionID, err := m.StartRemoteBootstrap("tablet-2", "peer-a", remotebootstrap.Manifest{
		Files: []remotebootstrap.FileMeta{{Name: "f", Data: []byte("data")}},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	chunk, _, err := m.FetchFileChunk(sessionID, "f", 0, 4)
	if err != nil {
		t.Fatalf("fetch chunk: %v", err)
	}
	if err := m.StageFileChunk(sessionID, "f", chunk, 1); err == nil {
		t.Fatalf("expected checksum mismatch error")
	}
}

func TestRemoteBootstrapFinalizeMissingFile(t *testing.T) {
	m := remotebootstrap.NewManager()
	sessionID, err := m.StartRemoteBootstrap("tablet-3", "peer-a", remotebootstrap.Manifest{
		Files: []remotebootstrap.FileMeta{{Name: "f1", Data: []byte("x")}, {Name: "f2", Data: []byte("y")}},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	chunk, crc, err := m.FetchFileChunk(sessionID, "f1", 0, 1)
	if err != nil {
		t.Fatalf("fetch chunk: %v", err)
	}
	if err := m.StageFileChunk(sessionID, "f1", chunk, crc); err != nil {
		t.Fatalf("stage f1: %v", err)
	}
	if err := m.FinalizeBootstrap(sessionID); err == nil {
		t.Fatalf("expected finalize failure for missing file")
	}
}
