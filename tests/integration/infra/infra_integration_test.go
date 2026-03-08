//go:build integration

package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"GoMultiDB/internal/replication/cdc"
	"GoMultiDB/internal/replication/controlplane"
	"GoMultiDB/internal/replication/xcluster"
	"GoMultiDB/internal/query/cql"
	"GoMultiDB/internal/query/sql"
)

type noopApplier struct{}

func (noopApplier) Apply(_ context.Context, _ cdc.Event) error { return nil }

func TestInfraCriticalPaths(t *testing.T) {
	repoRoot := mustFindRepoRoot(t)
	h := NewHarness(repoRoot)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	summary := Summary{
		StartedAt: time.Now().UTC(),
		Scenarios: map[string]string{},
		Details:   []string{},
	}

	if err := h.ComposeUp(ctx); err != nil {
		summary.Scenarios["compose_up"] = "failed"
		summary.Details = append(summary.Details, fmt.Sprintf("compose up failed: %v", err))
		_, _ = h.WriteSummary(summary)
		t.Fatalf("compose up: %v", err)
	}
	defer func() {
		_, _ = h.CaptureLogs(context.Background())
		_ = h.ComposeDown(context.Background())
	}()

	if err := h.WaitHealthy(ctx, "multidb-master", "multidb-tserver-1", "multidb-tserver-2", "multidb-tserver-3"); err != nil {
		summary.Scenarios["wait_healthy"] = "failed"
		summary.Details = append(summary.Details, fmt.Sprintf("wait healthy failed: %v", err))
		_, _ = h.WriteSummary(summary)
		t.Fatalf("wait healthy: %v", err)
	}
	summary.Scenarios["compose_lifecycle"] = "passed"

	run := func(name string, fn func() error) {
		t.Helper()
		if err := fn(); err != nil {
			summary.Scenarios[name] = "failed"
			summary.Details = append(summary.Details, fmt.Sprintf("%s: %v", name, err))
			_, _ = h.WriteSummary(summary)
			t.Fatalf("%s: %v", name, err)
		}
		summary.Scenarios[name] = "passed"
	}

	run("cdc_stream_lifecycle", func() error {
		store := cdc.NewStore()
		svc, err := cdc.NewService(store)
		if err != nil {
			return err
		}
		if err := svc.CreateStream(ctx, "s1", "t1"); err != nil {
			return err
		}
		for i := 1; i <= 3; i++ {
			if err := store.AppendEvent(ctx, cdc.Event{StreamID: "s1", TabletID: "t1", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
				return err
			}
		}
		out, err := svc.GetChanges(ctx, cdc.GetChangesRequest{StreamID: "s1", AfterSeq: 1, MaxRecords: 10})
		if err != nil {
			return err
		}
		if out.LatestSeen != 3 || len(out.Events) != 2 {
			return fmt.Errorf("unexpected getchanges output latest=%d len=%d", out.LatestSeen, len(out.Events))
		}
		if err := svc.SetCheckpoint(ctx, cdc.Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: out.LatestSeen}); err != nil {
			return err
		}
		if err := svc.DeleteStream(ctx, "s1"); err != nil {
			return err
		}
		return nil
	})

	run("xcluster_apply_e2e", func() error {
		store := cdc.NewStore()
		for i := 1; i <= 5; i++ {
			if err := store.AppendEvent(ctx, cdc.Event{StreamID: "sx", TabletID: "tx", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
				return err
			}
		}
		loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
		if err != nil {
			return err
		}
		poll, err := store.Poll(ctx, cdc.PollRequest{StreamID: "sx", TabletID: "tx", AfterSeq: 0, MaxRecords: 10})
		if err != nil {
			return err
		}
		if err := loop.ApplyBatch(ctx, poll.Events); err != nil {
			return err
		}
		cp, err := store.GetCheckpoint(ctx, "sx", "tx")
		if err != nil {
			return err
		}
		if cp.Sequence != 5 {
			return fmt.Errorf("expected checkpoint 5 got %d", cp.Sequence)
		}
		return nil
	})

	run("controlplane_state_transitions", func() error {
		r := controlplane.NewRegistry()
		if err := r.CreateStream(ctx, "s1", "t1"); err != nil {
			return err
		}
		if err := r.CreateJob(ctx, "j1", "s1", "cluster-b"); err != nil {
			return err
		}
		if err := r.PauseJob(ctx, "j1"); err != nil {
			return err
		}
		if err := r.ResumeJob(ctx, "j1"); err != nil {
			return err
		}
		if err := r.StopJob(ctx, "j1"); err != nil {
			return err
		}
		return nil
	})

	run("query_layer_smoke", func() error {
		coord := sql.NewLocalCoordinator()
		if err := coord.Start(ctx, sql.ProcessConfig{Enabled: true, BindAddress: "127.0.0.1:55433"}); err != nil {
			return err
		}
		defer func() { _ = coord.Stop(context.Background()) }()

		srv := cql.NewLocalServer()
		if err := srv.Start(ctx, cql.Config{Enabled: true, BindAddress: "127.0.0.1:59042", MaxConnections: 8}); err != nil {
			return err
		}
		defer func() { _ = srv.Stop(context.Background()) }()
		if err := srv.OpenConnection(ctx, "c1"); err != nil {
			return err
		}
		_, err := srv.Route(ctx, cql.Request{ConnID: "c1", Query: "SELECT now()"})
		return err
	})

	run("restart_recovery_persistence", func() error {
		tmp := t.TempDir()
		cpPath := filepath.Join(tmp, "checkpoints.json")
		regPath := filepath.Join(tmp, "registry.json")

		cpStore, err := cdc.NewFileCheckpointStore(cpPath)
		if err != nil {
			return err
		}
		if err := cpStore.AdvanceCheckpoint(ctx, cdc.Checkpoint{StreamID: "s1", TabletID: "t1", Sequence: 4}); err != nil {
			return err
		}
		reloadedCP, err := cdc.NewFileCheckpointStore(cpPath)
		if err != nil {
			return err
		}
		cp, err := reloadedCP.GetCheckpoint(ctx, "s1", "t1")
		if err != nil {
			return err
		}
		if cp.Sequence != 4 {
			return fmt.Errorf("expected reloaded checkpoint 4 got %d", cp.Sequence)
		}

		r, err := controlplane.NewRegistryWithFile(regPath)
		if err != nil {
			return err
		}
		if err := r.CreateStream(ctx, "sr", "tr"); err != nil {
			return err
		}
		r2, err := controlplane.NewRegistryWithFile(regPath)
		if err != nil {
			return err
		}
		streams, err := r2.ListStreams(ctx)
		if err != nil {
			return err
		}
		if len(streams) != 1 || streams[0].ID != "sr" {
			return fmt.Errorf("registry recovery mismatch: %+v", streams)
		}
		return nil
	})

	run("failure_recovery_restart_during_apply", func() error {
		store := cdc.NewStore()
		for i := 1; i <= 4; i++ {
			if err := store.AppendEvent(ctx, cdc.Event{StreamID: "sf", TabletID: "tf", Sequence: uint64(i), TimestampUTC: time.Now().UTC()}); err != nil {
				return err
			}
		}
		loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
		if err != nil {
			return err
		}

		if err := h.RestartService(ctx, "tserver-1"); err != nil {
			return err
		}
		if err := h.WaitHealthy(ctx, "multidb-tserver-1"); err != nil {
			return err
		}

		poll, err := store.Poll(ctx, cdc.PollRequest{StreamID: "sf", TabletID: "tf", AfterSeq: 0, MaxRecords: 10})
		if err != nil {
			return err
		}
		if err := loop.ApplyBatch(ctx, poll.Events); err != nil {
			return err
		}
		return nil
	})

	run("failure_recovery_scheduler_fault", func() error {
		r := controlplane.NewRegistry()
		if err := r.CreateStream(ctx, "ss", "tt"); err != nil {
			return err
		}
		if err := r.CreateJob(ctx, "jj", "ss", "cluster-b"); err != nil {
			return err
		}
		store := cdc.NewStore()
		if err := store.AppendEvent(ctx, cdc.Event{StreamID: "ss", TabletID: "tt", Sequence: 1, TimestampUTC: time.Now().UTC()}); err != nil {
			return err
		}
		loop, err := xcluster.NewLoop(xcluster.Config{}, store, noopApplier{})
		if err != nil {
			return err
		}
		sch, err := controlplane.NewScheduler(controlplane.SchedulerConfig{PerJobInFlightCap: 8, PollBatchSize: 8, FailureEveryTicks: 2}, r, store, loop)
		if err != nil {
			return err
		}
		if err := sch.Tick(ctx); err != nil {
			return err
		}
		if err := sch.Tick(ctx); err == nil {
			return fmt.Errorf("expected deterministic fault on second tick")
		}
		if err := sch.Tick(ctx); err != nil {
			return err
		}
		return nil
	})

	if err := writeInfraSnapshots(ctx, h); err != nil {
		summary.Details = append(summary.Details, fmt.Sprintf("snapshot capture warning: %v", err))
	}

	summary.Passed = true
	summary.FinishedAt = time.Now().UTC()
	if _, err := h.WriteSummary(summary); err != nil {
		t.Fatalf("write summary: %v", err)
	}
}

func writeInfraSnapshots(ctx context.Context, h *Harness) error {
	target := filepath.Join(h.ArtifactsDir, "status-snapshot.json")
	if err := h.EnsureArtifactsDir(); err != nil {
		return err
	}
	m := map[string]any{
		"captured_at": time.Now().UTC(),
		"note":        "infra status snapshot placeholder with compose context",
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(target, b, 0o644); err != nil {
		return err
	}
	_, _ = h.CaptureLogs(ctx)
	return nil
}

func mustFindRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	curr := wd
	for {
		if _, err := os.Stat(filepath.Join(curr, "go.mod")); err == nil {
			return curr
		}
		parent := filepath.Dir(curr)
		if parent == curr {
			t.Fatalf("could not find repo root from %s", wd)
		}
		curr = parent
	}
}
