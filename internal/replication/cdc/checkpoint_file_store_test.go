package cdc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
)

func TestFileCheckpointStorePersistsAndReloads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoints.json")

	store, err := NewFileCheckpointStore(path)
	if err != nil {
		t.Fatalf("new file checkpoint store: %v", err)
	}

	if err := store.AdvanceCheckpoint(context.Background(), Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 7}); err != nil {
		t.Fatalf("advance checkpoint: %v", err)
	}

	reloaded, err := NewFileCheckpointStore(path)
	if err != nil {
		t.Fatalf("reload file checkpoint store: %v", err)
	}
	cp, err := reloaded.GetCheckpoint(context.Background(), "s1", "t1")
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if cp.Sequence != 7 {
		t.Fatalf("expected persisted sequence 7, got %d", cp.Sequence)
	}
}

func TestFileCheckpointStoreMonotonicRules(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")

	store, err := NewFileCheckpointStore(path)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if err := store.AdvanceCheckpoint(context.Background(), Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 10}); err != nil {
		t.Fatalf("advance 10: %v", err)
	}
	if err := store.AdvanceCheckpoint(context.Background(), Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 10}); err != nil {
		t.Fatalf("idempotent 10: %v", err)
	}

	err = store.AdvanceCheckpoint(context.Background(), Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 9})
	if err == nil {
		t.Fatalf("expected regression conflict")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrConflict {
		t.Fatalf("expected conflict code, got %s", n.Code)
	}
}

func TestFileCheckpointStoreValidation(t *testing.T) {
	if _, err := NewFileCheckpointStore(""); err == nil {
		t.Fatalf("expected validation error for empty file path")
	}
}

func TestFileCheckpointStoreInjectedWriteFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoints.json")

	store, err := NewFileCheckpointStore(path)
	if err != nil {
		t.Fatalf("new file checkpoint store: %v", err)
	}
	store.hooks.writeFile = func(_ string, _ []byte, _ os.FileMode) error {
		return errors.New("injected write failure")
	}

	err = store.AdvanceCheckpoint(context.Background(), Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 1})
	if err == nil {
		t.Fatalf("expected injected write failure")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrInternal {
		t.Fatalf("expected internal code, got %s", n.Code)
	}
}

func TestFileCheckpointStoreInjectedRenameFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoints.json")

	store, err := NewFileCheckpointStore(path)
	if err != nil {
		t.Fatalf("new file checkpoint store: %v", err)
	}
	store.hooks.rename = func(_, _ string) error {
		return errors.New("injected rename failure")
	}

	err = store.AdvanceCheckpoint(context.Background(), Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 1})
	if err == nil {
		t.Fatalf("expected injected rename failure")
	}
	n := dberrors.NormalizeError(err)
	if n.Code != dberrors.ErrInternal {
		t.Fatalf("expected internal code, got %s", n.Code)
	}
}
