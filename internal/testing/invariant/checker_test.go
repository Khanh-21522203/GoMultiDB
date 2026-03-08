package invariant_test

import (
	"fmt"
	"testing"
	"time"

	"GoMultiDB/internal/testing/invariant"
)

// stubInspector is a controllable ClusterInspector for tests.
type stubInspector struct {
	store        map[string][]byte
	replicaStates []invariant.TabletReplicaState
	pendingIntents []string
	appliedOps   map[string]map[uint64]int
}

func newStub() *stubInspector {
	return &stubInspector{
		store:      make(map[string][]byte),
		appliedOps: make(map[string]map[uint64]int),
	}
}

func (s *stubInspector) ReadKey(key string) ([]byte, error) {
	return s.store[key], nil
}

func (s *stubInspector) TabletReplicaStates() ([]invariant.TabletReplicaState, error) {
	return s.replicaStates, nil
}

func (s *stubInspector) ListPendingIntents() ([]string, error) {
	return s.pendingIntents, nil
}

func (s *stubInspector) ListAppliedOpIDs(nodeID string) (map[uint64]int, error) {
	if m, ok := s.appliedOps[nodeID]; ok {
		return m, nil
	}
	return map[uint64]int{}, nil
}

// ── no_lost_writes ─────────────────────────────────────────────────────────────

func TestAssertNoLostWritesPass(t *testing.T) {
	insp := newStub()
	insp.store["key1"] = []byte("val1")
	insp.store["key2"] = []byte("val2")

	writes := []invariant.WriteRecord{
		{Key: "key1", Value: []byte("val1")},
		{Key: "key2", Value: []byte("val2")},
	}
	if err := invariant.AssertNoLostWrites(writes, insp); err != nil {
		t.Fatalf("expected pass: %v", err)
	}
}

func TestAssertNoLostWritesFail(t *testing.T) {
	insp := newStub()
	insp.store["key1"] = []byte("wrong")

	writes := []invariant.WriteRecord{{Key: "key1", Value: []byte("val1")}}
	err := invariant.AssertNoLostWrites(writes, insp)
	if err == nil {
		t.Fatalf("expected failure for wrong value")
	}
}

func TestAssertNoLostWritesMissingKey(t *testing.T) {
	insp := newStub()
	writes := []invariant.WriteRecord{{Key: "missing", Value: []byte("v")}}
	err := invariant.AssertNoLostWrites(writes, insp)
	if err == nil {
		t.Fatalf("expected failure for missing key")
	}
}

// ── replica_convergence ────────────────────────────────────────────────────────

func TestAssertReplicaConvergencePass(t *testing.T) {
	insp := newStub()
	now := time.Now()
	insp.replicaStates = []invariant.TabletReplicaState{
		{NodeID: "n1", TabletID: "tab-1", LastOpID: 100, Timestamp: now},
		{NodeID: "n2", TabletID: "tab-1", LastOpID: 100, Timestamp: now},
	}
	if err := invariant.AssertReplicaConvergence(insp, 5*time.Second); err != nil {
		t.Fatalf("expected convergence: %v", err)
	}
}

func TestAssertReplicaConvergenceStaleLag(t *testing.T) {
	insp := newStub()
	old := time.Now().Add(-10 * time.Second) // older than tolerance
	insp.replicaStates = []invariant.TabletReplicaState{
		{NodeID: "n1", TabletID: "tab-1", LastOpID: 100, Timestamp: time.Now()},
		{NodeID: "n2", TabletID: "tab-1", LastOpID: 90, Timestamp: old},
	}
	if err := invariant.AssertReplicaConvergence(insp, 5*time.Second); err == nil {
		t.Fatalf("expected convergence failure for stale lagging replica")
	}
}

func TestAssertReplicaConvergenceRecentLag(t *testing.T) {
	insp := newStub()
	recent := time.Now().Add(-1 * time.Second) // within tolerance
	insp.replicaStates = []invariant.TabletReplicaState{
		{NodeID: "n1", TabletID: "tab-1", LastOpID: 100, Timestamp: time.Now()},
		{NodeID: "n2", TabletID: "tab-1", LastOpID: 90, Timestamp: recent},
	}
	// Within tolerance → should pass.
	if err := invariant.AssertReplicaConvergence(insp, 5*time.Second); err != nil {
		t.Fatalf("recent lag within tolerance should pass: %v", err)
	}
}

// ── no_uncommitted_intents ─────────────────────────────────────────────────────

func TestAssertNoUncommittedIntentsPass(t *testing.T) {
	insp := newStub()
	if err := invariant.AssertNoUncommittedIntents(insp); err != nil {
		t.Fatalf("expected no intents: %v", err)
	}
}

func TestAssertNoUncommittedIntentsFail(t *testing.T) {
	insp := newStub()
	insp.pendingIntents = []string{"txn-abc"}
	if err := invariant.AssertNoUncommittedIntents(insp); err == nil {
		t.Fatalf("expected failure with pending intents")
	}
}

// ── no_double_apply ────────────────────────────────────────────────────────────

func TestAssertNoDoubleApplyPass(t *testing.T) {
	insp := newStub()
	insp.appliedOps["node-1"] = map[uint64]int{1: 1, 2: 1, 3: 1}
	if err := invariant.AssertNoDoubleApply([]string{"node-1"}, insp); err != nil {
		t.Fatalf("expected no double apply: %v", err)
	}
}

func TestAssertNoDoubleApplyFail(t *testing.T) {
	insp := newStub()
	insp.appliedOps["node-1"] = map[uint64]int{1: 1, 2: 2} // opID 2 applied twice
	if err := invariant.AssertNoDoubleApply([]string{"node-1"}, insp); err == nil {
		t.Fatalf("expected double-apply failure")
	}
}

// ── AssertInvariant dispatch ───────────────────────────────────────────────────

func TestAssertInvariantUnknown(t *testing.T) {
	insp := newStub()
	if err := invariant.AssertInvariant("unknown_invariant", insp); err == nil {
		t.Fatalf("expected error for unknown invariant")
	}
}

func TestAssertInvariantReplicaConvergence(t *testing.T) {
	insp := newStub()
	now := time.Now()
	insp.replicaStates = []invariant.TabletReplicaState{
		{NodeID: "n1", TabletID: "t1", LastOpID: 5, Timestamp: now},
	}
	if err := invariant.AssertInvariant("replica_convergence", insp); err != nil {
		t.Fatalf("single replica should converge: %v", err)
	}
}

// errorInspector returns errors for all operations.
type errorInspector struct{}

func (e *errorInspector) ReadKey(_ string) ([]byte, error) {
	return nil, fmt.Errorf("read error")
}
func (e *errorInspector) TabletReplicaStates() ([]invariant.TabletReplicaState, error) {
	return nil, fmt.Errorf("list error")
}
func (e *errorInspector) ListPendingIntents() ([]string, error) {
	return nil, fmt.Errorf("list error")
}
func (e *errorInspector) ListAppliedOpIDs(_ string) (map[uint64]int, error) {
	return nil, fmt.Errorf("list error")
}

func TestAssertInvariantPropagatorsInspectorError(t *testing.T) {
	ei := &errorInspector{}
	if err := invariant.AssertNoLostWrites([]invariant.WriteRecord{{Key: "k", Value: []byte("v")}}, ei); err == nil {
		t.Fatalf("expected error propagation from inspector")
	}
}
