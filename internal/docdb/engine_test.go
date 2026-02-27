package docdb_test

import (
	"context"
	"testing"

	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/docdb"
	"GoMultiDB/internal/storage/rocks"
)

func TestEngineApplyAndRead(t *testing.T) {
	s := rocks.NewMemoryStore()
	e, err := docdb.NewEngine(s)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	ctx := context.Background()
	err = e.ApplyNonTransactional(ctx, docdb.DocWriteBatch{Mutations: []docdb.KVMutation{{Key: []byte("k1"), Value: []byte("v1"), Op: docdb.MutationSet, WriteHT: 10}}})
	if err != nil {
		t.Fatalf("apply non txn: %v", err)
	}
	v, ok, err := e.Read(ctx, []byte("k1"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("unexpected read ok=%v val=%s err=%v", ok, string(v), err)
	}
}

func TestEngineWriteIntents(t *testing.T) {
	s := rocks.NewMemoryStore()
	e, err := docdb.NewEngine(s)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	txnID := ids.MustNewTxnID()
	err = e.WriteIntents(context.Background(), docdb.TxnMeta{TxnID: txnID}, docdb.DocWriteBatch{Mutations: []docdb.KVMutation{{Key: []byte("k2"), Value: []byte("v2"), Op: docdb.MutationSet}}})
	if err != nil {
		t.Fatalf("write intents: %v", err)
	}
	it, err := s.NewIterator(context.Background(), rocks.IntentsDB, txnID[:])
	if err != nil {
		t.Fatalf("iterator intents: %v", err)
	}
	if !it.Next() {
		t.Fatalf("expected at least one intent record")
	}
}

func TestEngineReadAtMVCCVisibility(t *testing.T) {
	s := rocks.NewMemoryStore()
	e, err := docdb.NewEngine(s)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	ctx := context.Background()

	batch := docdb.DocWriteBatch{Mutations: []docdb.KVMutation{
		{Key: []byte("user:1"), Value: []byte("v1"), Op: docdb.MutationSet, WriteHT: 100},
		{Key: []byte("user:1"), Value: []byte("v2"), Op: docdb.MutationSet, WriteHT: 200},
		{Key: []byte("user:1"), Value: []byte("v3"), Op: docdb.MutationSet, WriteHT: 300},
	}}
	if err := e.ApplyNonTransactional(ctx, batch); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	cases := []struct {
		readHT uint64
		wantOK bool
		wantV  string
	}{
		{readHT: 50, wantOK: false, wantV: ""},
		{readHT: 100, wantOK: true, wantV: "v1"},
		{readHT: 199, wantOK: true, wantV: "v1"},
		{readHT: 200, wantOK: true, wantV: "v2"},
		{readHT: 250, wantOK: true, wantV: "v2"},
		{readHT: 300, wantOK: true, wantV: "v3"},
		{readHT: 999, wantOK: true, wantV: "v3"},
	}

	for _, tc := range cases {
		got, ok, err := e.ReadAt(ctx, []byte("user:1"), tc.readHT)
		if err != nil {
			t.Fatalf("ReadAt(%d): %v", tc.readHT, err)
		}
		if ok != tc.wantOK {
			t.Fatalf("ReadAt(%d) ok=%v want=%v", tc.readHT, ok, tc.wantOK)
		}
		if ok && string(got) != tc.wantV {
			t.Fatalf("ReadAt(%d) val=%s want=%s", tc.readHT, string(got), tc.wantV)
		}
	}
}

func TestApplyAndRemoveIntentsLifecycle(t *testing.T) {
	s := rocks.NewMemoryStore()
	e, err := docdb.NewEngine(s)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	ctx := context.Background()
	txnID := ids.MustNewTxnID()

	batch := docdb.DocWriteBatch{Mutations: []docdb.KVMutation{
		{Key: []byte("k10"), Value: []byte("v10"), Op: docdb.MutationSet},
		{Key: []byte("k11"), Value: []byte("v11"), Op: docdb.MutationSet},
	}}
	if err := e.WriteIntents(ctx, docdb.TxnMeta{TxnID: txnID}, batch); err != nil {
		t.Fatalf("write intents: %v", err)
	}

	done, err := e.ApplyIntents(ctx, txnID, 500, 100)
	if err != nil {
		t.Fatalf("apply intents: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true")
	}

	v, ok, err := e.ReadAt(ctx, []byte("k10"), 500)
	if err != nil || !ok || string(v) != "v10" {
		t.Fatalf("post-apply read mismatch ok=%v val=%s err=%v", ok, string(v), err)
	}

	// write again and remove without applying
	txnID2 := ids.MustNewTxnID()
	if err := e.WriteIntents(ctx, docdb.TxnMeta{TxnID: txnID2}, docdb.DocWriteBatch{Mutations: []docdb.KVMutation{{Key: []byte("k12"), Value: []byte("v12"), Op: docdb.MutationSet}}}); err != nil {
		t.Fatalf("write intents 2: %v", err)
	}
	done, err = e.RemoveIntents(ctx, txnID2, 100)
	if err != nil {
		t.Fatalf("remove intents: %v", err)
	}
	if !done {
		t.Fatalf("expected done=true on remove")
	}
	_, ok, err = e.Read(ctx, []byte("k12"))
	if err != nil {
		t.Fatalf("read k12: %v", err)
	}
	if ok {
		t.Fatalf("k12 should not be visible after abort cleanup")
	}
}
