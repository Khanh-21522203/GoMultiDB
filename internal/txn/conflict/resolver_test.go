package conflict_test

import (
	"context"
	"errors"
	"testing"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/txn/conflict"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func mustTxnID(b byte) ids.TxnID {
	var id ids.TxnID
	id[0] = b
	return id
}

// staticScanner returns a fixed intent list for every ScanIntents call.
type staticScanner struct{ intents []conflict.Intent }

func (s *staticScanner) ScanIntents(_ context.Context, _ [][]byte) ([]conflict.Intent, error) {
	return s.intents, nil
}

// errorScanner always returns an error from ScanIntents.
type errorScanner struct{}

func (e *errorScanner) ScanIntents(_ context.Context, _ [][]byte) ([]conflict.Intent, error) {
	return nil, errors.New("disk error")
}

// mapStatusSource looks up statuses from a pre-populated map.
type mapStatusSource struct{ m map[ids.TxnID]conflict.StatusResult }

func (s *mapStatusSource) GetStatus(_ context.Context, id ids.TxnID) (conflict.StatusResult, error) {
	if r, ok := s.m[id]; ok {
		return r, nil
	}
	return conflict.StatusResult{Status: conflict.StatusUnknown}, nil
}

// errorStatusSource always returns an error.
type errorStatusSource struct{}

func (e *errorStatusSource) GetStatus(_ context.Context, _ ids.TxnID) (conflict.StatusResult, error) {
	return conflict.StatusResult{}, errors.New("rpc error")
}

func noopAbort(_ context.Context, _ ids.TxnID, _ string) error { return nil }

func errorAbort(_ context.Context, _ ids.TxnID, _ string) error { return errors.New("abort rpc failed") }

func errCode(t *testing.T, err error) dberrors.ErrorCode {
	t.Helper()
	var dbe dberrors.DBError
	if !errors.As(err, &dbe) {
		t.Fatalf("expected DBError, got %T: %v", err, err)
	}
	return dbe.Code
}

// ── no-conflict cases ─────────────────────────────────────────────────────────

func TestNoIntents(t *testing.T) {
	r := conflict.New(&staticScanner{}, &mapStatusSource{m: nil}, noopAbort, conflict.Config{})
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestSelfIntentIgnored(t *testing.T) {
	me := mustTxnID(1)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: me, Priority: 5}}}
	r := conflict.New(scanner, &mapStatusSource{m: nil}, noopAbort, conflict.Config{})
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, me, 5, 100)
	if err != nil {
		t.Fatalf("own intent should be ignored, got %v", err)
	}
}

// ── ABORTED intent ────────────────────────────────────────────────────────────

func TestAbortedIntentIgnored(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other, Priority: 5}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusAborted},
	}}
	r := conflict.New(scanner, src, noopAbort, conflict.Config{})
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err != nil {
		t.Fatalf("aborted intent should be ignored, got %v", err)
	}
}

// ── COMMITTED intent ──────────────────────────────────────────────────────────

func TestCommittedIntentOlderThanSnapshot(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusCommitted, CommitHT: 50},
	}}
	r := conflict.New(scanner, src, noopAbort, conflict.Config{})
	// snapshot_ht=100 > commit_ht=50 → no conflict
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err != nil {
		t.Fatalf("committed intent older than snapshot should not conflict, got %v", err)
	}
}

func TestCommittedIntentNewerThanSnapshot(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusCommitted, CommitHT: 200},
	}}
	r := conflict.New(scanner, src, noopAbort, conflict.Config{})
	// snapshot_ht=100 < commit_ht=200 → restart required
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected ErrTxnRestartRequired")
	}
	if got := errCode(t, err); got != dberrors.ErrTxnRestartRequired {
		t.Fatalf("expected ErrTxnRestartRequired, got %v", got)
	}
}

func TestCommittedIntentExactlyEqualSnapshot(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusCommitted, CommitHT: 100},
	}}
	r := conflict.New(scanner, src, noopAbort, conflict.Config{})
	// commit_ht == snapshot_ht → our snapshot captures it, no conflict
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err != nil {
		t.Fatalf("commit_ht == snapshot_ht should not conflict, got %v", err)
	}
}

// ── PENDING intent — priority-based preemption ─────────────────────────────

func TestPendingLowerPriorityConflictingTxnAborted(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other, Priority: 3}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusPending, Priority: 3},
	}}
	aborted := false
	abortFn := func(_ context.Context, id ids.TxnID, _ string) error {
		if id == other {
			aborted = true
		}
		return nil
	}
	r := conflict.New(scanner, src, abortFn, conflict.Config{})
	// myPriority=10 > conflictPriority=3 → abort the other txn
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected ErrConflict (retry after abort)")
	}
	if got := errCode(t, err); got != dberrors.ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", got)
	}
	if !aborted {
		t.Fatalf("expected lower-priority txn to be aborted")
	}
}

func TestPendingHigherPriorityBlocksCaller(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other, Priority: 50}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusPending, Priority: 50},
	}}
	r := conflict.New(scanner, src, noopAbort, conflict.Config{})
	// myPriority=5 < conflictPriority=50 → caller must abort itself
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 5, 100)
	if err == nil {
		t.Fatalf("expected ErrConflict (caller should abort)")
	}
	if got := errCode(t, err); got != dberrors.ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", got)
	}
	// must NOT be retryable — caller needs to abort, not retry
	var dbe dberrors.DBError
	if errors.As(err, &dbe) && dbe.Retryable {
		t.Fatalf("conflict with higher-priority txn must not be marked retryable")
	}
}

func TestPendingEqualPriorityAbortConflicting(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other, Priority: 10}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusPending, Priority: 10},
	}}
	aborted := false
	abortFn := func(_ context.Context, id ids.TxnID, _ string) error {
		if id == other {
			aborted = true
		}
		return nil
	}
	r := conflict.New(scanner, src, abortFn, conflict.Config{})
	// equal priority: abort the conflicting txn (preempt by arrival order tiebreak)
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected ErrConflict (retry)")
	}
	if !aborted {
		t.Fatalf("equal-priority conflict should abort conflicting txn")
	}
}

// ── wait-queue mode ───────────────────────────────────────────────────────────

func TestWaitQueueModeReturnsPending(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusPending, Priority: 1},
	}}
	r := conflict.New(scanner, src, noopAbort, conflict.Config{UseWaitQueue: true})
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected retryable error in wait-queue mode")
	}
	if got := errCode(t, err); got != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected ErrRetryableUnavailable in wait-queue mode, got %v", got)
	}
}

// ── error propagation ─────────────────────────────────────────────────────────

func TestScanIntentsError(t *testing.T) {
	r := conflict.New(&errorScanner{}, &mapStatusSource{m: nil}, noopAbort, conflict.Config{})
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected error from scanner failure")
	}
	if got := errCode(t, err); got != dberrors.ErrInternal {
		t.Fatalf("expected ErrInternal, got %v", got)
	}
}

func TestStatusSourceError(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other}}}
	r := conflict.New(scanner, &errorStatusSource{}, noopAbort, conflict.Config{})
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected error from status source failure")
	}
	if got := errCode(t, err); got != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected ErrRetryableUnavailable, got %v", got)
	}
}

func TestAbortFnError(t *testing.T) {
	other := mustTxnID(2)
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other, Priority: 1}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusPending, Priority: 1},
	}}
	r := conflict.New(scanner, src, errorAbort, conflict.Config{})
	// myPriority=10 > conflictPriority=1 → abort fn called but it fails
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected error when abort fn fails")
	}
	if got := errCode(t, err); got != dberrors.ErrRetryableUnavailable {
		t.Fatalf("expected ErrRetryableUnavailable when abort fails, got %v", got)
	}
}

// ── unknown status treated as pending ────────────────────────────────────────

func TestUnknownStatusTreatedAsPending(t *testing.T) {
	other := mustTxnID(2)
	// intent has priority set; status source returns unknown
	scanner := &staticScanner{intents: []conflict.Intent{{TxnID: other, Priority: 1}}}
	src := &mapStatusSource{m: map[ids.TxnID]conflict.StatusResult{
		other: {Status: conflict.StatusUnknown},
	}}
	aborted := false
	r := conflict.New(scanner, src, func(_ context.Context, id ids.TxnID, _ string) error {
		if id == other {
			aborted = true
		}
		return nil
	}, conflict.Config{})
	err := r.Check(context.Background(), [][]byte{[]byte("key")}, mustTxnID(1), 10, 100)
	if err == nil {
		t.Fatalf("expected ErrConflict for unknown-status conflict")
	}
	if !aborted {
		t.Fatalf("unknown-status lower-priority intent should be aborted")
	}
}
