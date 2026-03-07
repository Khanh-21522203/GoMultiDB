package controlplane

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/replication/cdc"
	"GoMultiDB/internal/replication/xcluster"
)

type noopApplier struct{}

func (noopApplier) Apply(_ context.Context, _ cdc.Event) error { return nil }

func TestStreamAndJobTransitions(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.PauseStream(ctx, "s1"); err != nil {
		t.Fatalf("pause stream: %v", err)
	}
	if err := r.ResumeStream(ctx, "s1"); err != nil {
		t.Fatalf("resume stream: %v", err)
	}
	if err := r.StopStream(ctx, "s1"); err != nil {
		t.Fatalf("stop stream: %v", err)
	}
	if err := r.ResumeStream(ctx, "s1"); err == nil {
		t.Fatalf("expected conflict resuming stopped stream")
	}

	if err := r.CreateStream(ctx, "s2", "t2"); err != nil {
		t.Fatalf("create stream s2: %v", err)
	}
	if err := r.CreateJob(ctx, "j1", "s2", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := r.PauseJob(ctx, "j1"); err != nil {
		t.Fatalf("pause job: %v", err)
	}
	if err := r.ResumeJob(ctx, "j1"); err != nil {
		t.Fatalf("resume job: %v", err)
	}
	if err := r.StopJob(ctx, "j1"); err != nil {
		t.Fatalf("stop job: %v", err)
	}
	if err := r.ResumeJob(ctx, "j1"); err == nil {
		t.Fatalf("expected conflict resuming stopped job")
	}
}

func TestInvalidTransitionsUseCanonicalErrors(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	if err := r.CreateStream(ctx, "", "t1"); err == nil {
		t.Fatalf("expected invalid argument for empty stream id")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}

	if err := r.PauseJob(ctx, "missing"); err == nil {
		t.Fatalf("expected invalid argument for missing job")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}

	if err := r.CreateJob(ctx, "j1", "missing-stream", "cluster-b"); err == nil {
		t.Fatalf("expected invalid argument for missing stream")
	} else if n := dberrors.NormalizeError(err); n.Code != dberrors.ErrInvalidArgument {
		t.Fatalf("expected invalid argument code, got %s", n.Code)
	}
}

func TestSnapshotDeterminism(t *testing.T) {
	ctx := context.Background()

	build := func() (Snapshot, error) {
		r := NewRegistry()
		store := cdc.NewStore()
		loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
		if err != nil {
			return Snapshot{}, err
		}
		if err := r.CreateStream(ctx, "sdet", "tdet"); err != nil {
			return Snapshot{}, err
		}
		if err := r.CreateJob(ctx, "jdet", "sdet", "cluster-b"); err != nil {
			return Snapshot{}, err
		}
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "sdet", TabletID: "tdet", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
			return Snapshot{}, err
		}
		if err := loop.ApplyEvent(ctx, cdc.Event{StreamID: "sdet", TabletID: "tdet", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
			return Snapshot{}, err
		}
		return r.Snapshot(ctx, store, loop)
	}

	s1, err := build()
	if err != nil {
		t.Fatalf("build snapshot 1: %v", err)
	}
	s2, err := build()
	if err != nil {
		t.Fatalf("build snapshot 2: %v", err)
	}

	if len(s1.Streams) != 1 || len(s2.Streams) != 1 {
		t.Fatalf("expected one stream in both snapshots")
	}
	if s1.Streams[0].ID != s2.Streams[0].ID || s1.Streams[0].Checkpoint != s2.Streams[0].Checkpoint || s1.Streams[0].LagEvents != s2.Streams[0].LagEvents {
		t.Fatalf("stream snapshot determinism mismatch")
	}
	if len(s1.Jobs) != 1 || len(s2.Jobs) != 1 || s1.Jobs[0].ID != s2.Jobs[0].ID {
		t.Fatalf("job snapshot determinism mismatch")
	}
}

func TestRegistryPersistenceRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "registry.json")

	r, err := NewRegistryWithFile(path)
	if err != nil {
		t.Fatalf("new registry with file: %v", err)
	}
	if err := r.CreateStream(ctx, "sp", "tp"); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if err := r.CreateJob(ctx, "jp", "sp", "cluster-b"); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := r.PauseJob(ctx, "jp"); err != nil {
		t.Fatalf("pause job: %v", err)
	}

	r2, err := NewRegistryWithFile(path)
	if err != nil {
		t.Fatalf("reload registry: %v", err)
	}
	streams, err := r2.ListStreams(ctx)
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	jobs, err := r2.ListJobs(ctx)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(streams) != 1 || streams[0].ID != "sp" {
		t.Fatalf("unexpected streams after reload: %+v", streams)
	}
	if len(jobs) != 1 || jobs[0].ID != "jp" || jobs[0].State != JobStatePaused {
		t.Fatalf("unexpected jobs after reload: %+v", jobs)
	}
}
