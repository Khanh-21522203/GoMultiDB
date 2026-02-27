package txn_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/txn"
)

type fakeApplier struct {
	applyCalls  int
	removeCalls int
	applyDone   bool
	removeDone  bool
	applyErr    error
	removeErr   error
}

func (f *fakeApplier) ApplyIntents(_ context.Context, _ [16]byte, _ uint64, _ int) (bool, error) {
	f.applyCalls++
	if f.applyErr != nil {
		return false, f.applyErr
	}
	if f.applyDone {
		return true, nil
	}
	f.applyDone = true
	return false, nil
}

func (f *fakeApplier) RemoveIntents(_ context.Context, _ [16]byte, _ int) (bool, error) {
	f.removeCalls++
	if f.removeErr != nil {
		return false, f.removeErr
	}
	if f.removeDone {
		return true, nil
	}
	f.removeDone = true
	return false, nil
}

func TestManagerBeginCommitIdempotent(t *testing.T) {
	m := txn.NewManager(txn.Config{}, nil)
	txnID := ids.MustNewTxnID()
	reqID := ids.MustNewRequestID()
	ctx := context.Background()

	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("idempotent begin failed: %v", err)
	}

	ht, err := m.Commit(ctx, txnID, reqID, 123)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if ht != 123 {
		t.Fatalf("expected commit ht 123, got %d", ht)
	}

	ht2, err := m.Commit(ctx, txnID, reqID, 999)
	if err != nil {
		t.Fatalf("idempotent commit: %v", err)
	}
	if ht2 != 123 {
		t.Fatalf("expected original commit ht 123, got %d", ht2)
	}
}

func TestManagerAbort(t *testing.T) {
	m := txn.NewManager(txn.Config{}, nil)
	txnID := ids.MustNewTxnID()
	reqID := ids.MustNewRequestID()
	ctx := context.Background()

	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := m.Abort(ctx, txnID, reqID); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if err := m.Abort(ctx, txnID, reqID); err != nil {
		t.Fatalf("idempotent abort: %v", err)
	}
}

func TestManagerTimeoutExpiration(t *testing.T) {
	now := time.Now().UTC()
	m := txn.NewManager(txn.Config{
		TxnTimeout: 100 * time.Millisecond,
		NowFn:      func() time.Time { return now },
	}, nil)
	txnID := ids.MustNewTxnID()
	reqID := ids.MustNewRequestID()
	ctx := context.Background()

	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("begin: %v", err)
	}
	now = now.Add(200 * time.Millisecond)

	_, err := m.Commit(ctx, txnID, reqID, 10)
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	var dbErr dberrors.DBError
	if ok := asDBError(err, &dbErr); !ok {
		t.Fatalf("expected DBError, got %T", err)
	}
	if dbErr.Code != dberrors.ErrTimeout {
		t.Fatalf("expected timeout code, got %s", dbErr.Code)
	}
}

func TestManagerIdempotencyMismatch(t *testing.T) {
	m := txn.NewManager(txn.Config{}, nil)
	txnID := ids.MustNewTxnID()
	reqID := ids.MustNewRequestID()
	otherReqID := ids.MustNewRequestID()
	ctx := context.Background()

	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err := m.Commit(ctx, txnID, otherReqID, 10)
	if err == nil {
		t.Fatalf("expected idempotency conflict")
	}
	var dbErr dberrors.DBError
	if ok := asDBError(err, &dbErr); !ok {
		t.Fatalf("expected DBError")
	}
	if dbErr.Code != dberrors.ErrIdempotencyConflict {
		t.Fatalf("expected idempotency code, got %s", dbErr.Code)
	}
}

func TestExpireStale(t *testing.T) {
	now := time.Now().UTC()
	m := txn.NewManager(txn.Config{
		TxnTimeout: 100 * time.Millisecond,
		NowFn:      func() time.Time { return now },
	}, nil)
	ctx := context.Background()

	t1 := ids.MustNewTxnID()
	r1 := ids.MustNewRequestID()
	if err := m.Begin(ctx, t1, r1); err != nil {
		t.Fatalf("begin t1: %v", err)
	}

	now = now.Add(150 * time.Millisecond)
	expired := m.ExpireStale()
	if expired != 1 {
		t.Fatalf("expected 1 expired txn, got %d", expired)
	}
	rec, ok := m.Get(t1)
	if !ok || rec.State != txn.Aborted {
		t.Fatalf("expected aborted txn after expiration")
	}
}

func TestCommitCallsApplyIntents(t *testing.T) {
	ap := &fakeApplier{}
	m := txn.NewManager(txn.Config{ApplyBatchLimit: 2}, ap)
	ctx := context.Background()
	txnID := ids.MustNewTxnID()
	reqID := ids.MustNewRequestID()
	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err := m.Commit(ctx, txnID, reqID, 777)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if ap.applyCalls < 2 {
		t.Fatalf("expected chunked apply calls >=2, got %d", ap.applyCalls)
	}
}

func TestAbortCallsRemoveIntents(t *testing.T) {
	ap := &fakeApplier{}
	m := txn.NewManager(txn.Config{ApplyBatchLimit: 2}, ap)
	ctx := context.Background()
	txnID := ids.MustNewTxnID()
	reqID := ids.MustNewRequestID()
	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := m.Abort(ctx, txnID, reqID); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if ap.removeCalls < 2 {
		t.Fatalf("expected chunked remove calls >=2, got %d", ap.removeCalls)
	}
}

func TestCommitApplyErrorIsRetryable(t *testing.T) {
	ap := &fakeApplier{applyErr: errors.New("downstream")}
	m := txn.NewManager(txn.Config{}, ap)
	ctx := context.Background()
	txnID := ids.MustNewTxnID()
	reqID := ids.MustNewRequestID()
	if err := m.Begin(ctx, txnID, reqID); err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err := m.Commit(ctx, txnID, reqID, 10)
	if err == nil {
		t.Fatalf("expected commit error")
	}
	var dbErr dberrors.DBError
	if !asDBError(err, &dbErr) {
		t.Fatalf("expected DBError")
	}
	if dbErr.Code != dberrors.ErrRetryableUnavailable || !dbErr.Retryable {
		t.Fatalf("expected retryable unavailable, got code=%s retryable=%v", dbErr.Code, dbErr.Retryable)
	}
}

func asDBError(err error, target *dberrors.DBError) bool {
	if err == nil {
		return false
	}
	if v, ok := err.(dberrors.DBError); ok {
		*target = v
		return true
	}
	return false
}
