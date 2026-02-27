package docdb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"

	"GoMultiDB/internal/common/types"
	"GoMultiDB/internal/storage/rocks"
)

type MutationOp string

const (
	MutationSet MutationOp = "set"
)

type KVMutation struct {
	Key     []byte
	Value   []byte
	Op      MutationOp
	WriteHT uint64
}

type DocWriteBatch struct {
	Mutations []KVMutation
}

type TxnMeta struct {
	TxnID    [16]byte
	Priority uint64
	StartHT  types.HybridTime
}

type IntentRecord struct {
	TxnID [16]byte `json:"txn_id"`
	Key   []byte   `json:"key"`
	Value []byte   `json:"value"`
}

type Engine struct {
	store  rocks.Store
	nextHT atomic.Uint64
}

func NewEngine(store rocks.Store) (*Engine, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	e := &Engine{store: store}
	e.nextHT.Store(1)
	return e, nil
}

func (e *Engine) ApplyNonTransactional(ctx context.Context, batch DocWriteBatch) error {
	wb := rocks.WriteBatch{Ops: make([]rocks.KV, 0, len(batch.Mutations))}
	for _, m := range batch.Mutations {
		if m.Op != MutationSet {
			return fmt.Errorf("unsupported mutation op: %s", m.Op)
		}
		ht := m.WriteHT
		if ht == 0 {
			ht = e.nextHT.Add(1)
		}
		wb.Ops = append(wb.Ops, rocks.KV{Key: makeVersionedKey(m.Key, ht), Value: m.Value})
	}
	return e.store.ApplyWriteBatch(ctx, rocks.RegularDB, wb)
}

func (e *Engine) WriteIntents(ctx context.Context, txn TxnMeta, batch DocWriteBatch) error {
	wb := rocks.WriteBatch{Ops: make([]rocks.KV, 0, len(batch.Mutations)*2)}
	for _, m := range batch.Mutations {
		rec := IntentRecord{TxnID: txn.TxnID, Key: m.Key, Value: m.Value}
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		intentKey := makeIntentKey(txn.TxnID, m.Key)
		reverseKey := makeReverseIndexKey(txn.TxnID, m.Key)
		wb.Ops = append(wb.Ops,
			rocks.KV{Key: intentKey, Value: b},
			rocks.KV{Key: reverseKey, Value: intentKey},
		)
	}
	return e.store.ApplyWriteBatch(ctx, rocks.IntentsDB, wb)
}

func (e *Engine) ApplyIntents(ctx context.Context, txnID [16]byte, commitHT uint64, limit int) (done bool, err error) {
	if limit <= 0 {
		limit = math.MaxInt32
	}
	it, err := e.store.NewIterator(ctx, rocks.IntentsDB, reverseIndexPrefix(txnID))
	if err != nil {
		return false, err
	}

	toRegular := rocks.WriteBatch{Ops: make([]rocks.KV, 0)}
	deleteKeys := make([][]byte, 0)
	processed := 0

	for it.Next() {
		if processed >= limit {
			return false, e.store.ApplyWriteBatch(ctx, rocks.RegularDB, toRegular)
		}
		idx := it.Item()
		intentKey := idx.Value
		raw, ok, gerr := e.store.Get(ctx, rocks.IntentsDB, intentKey)
		if gerr != nil {
			return false, gerr
		}
		if !ok {
			continue
		}
		var rec IntentRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return false, err
		}
		toRegular.Ops = append(toRegular.Ops, rocks.KV{Key: makeVersionedKey(rec.Key, commitHT), Value: rec.Value})
		deleteKeys = append(deleteKeys, append([]byte(nil), intentKey...), append([]byte(nil), idx.Key...))
		processed++
	}
	if err := it.Err(); err != nil {
		return false, err
	}
	if err := e.store.ApplyWriteBatch(ctx, rocks.RegularDB, toRegular); err != nil {
		return false, err
	}
	if err := e.removeKeys(ctx, deleteKeys); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Engine) RemoveIntents(ctx context.Context, txnID [16]byte, limit int) (done bool, err error) {
	if limit <= 0 {
		limit = math.MaxInt32
	}
	it, err := e.store.NewIterator(ctx, rocks.IntentsDB, reverseIndexPrefix(txnID))
	if err != nil {
		return false, err
	}
	deleteKeys := make([][]byte, 0)
	processed := 0
	for it.Next() {
		if processed >= limit {
			return false, e.removeKeys(ctx, deleteKeys)
		}
		idx := it.Item()
		intentKey := idx.Value
		deleteKeys = append(deleteKeys, append([]byte(nil), intentKey...), append([]byte(nil), idx.Key...))
		processed++
	}
	if err := it.Err(); err != nil {
		return false, err
	}
	if err := e.removeKeys(ctx, deleteKeys); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Engine) removeKeys(ctx context.Context, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	return e.store.DeleteKeys(ctx, rocks.IntentsDB, keys)
}

func (e *Engine) Read(ctx context.Context, key []byte) ([]byte, bool, error) {
	return e.ReadAt(ctx, key, math.MaxUint64)
}

func (e *Engine) ReadAt(ctx context.Context, key []byte, readHT uint64) ([]byte, bool, error) {
	prefix := versionPrefix(key)
	it, err := e.store.NewIterator(ctx, rocks.RegularDB, prefix)
	if err != nil {
		return nil, false, err
	}

	var (
		bestHT uint64
		bestV  []byte
		found  bool
	)
	for it.Next() {
		kv := it.Item()
		ht, ok := parseVersionedHT(kv.Key)
		if !ok || ht > readHT {
			continue
		}
		if !found || ht > bestHT {
			found = true
			bestHT = ht
			bestV = append([]byte(nil), kv.Value...)
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	return bestV, true, nil
}

func makeIntentKey(txnID [16]byte, key []byte) []byte {
	out := make([]byte, 0, len(txnID)+1+len(key))
	out = append(out, txnID[:]...)
	out = append(out, '|')
	out = append(out, key...)
	return out
}

func makeReverseIndexKey(txnID [16]byte, key []byte) []byte {
	return []byte(fmt.Sprintf("ri|%s|%s", hex.EncodeToString(txnID[:]), key))
}

func reverseIndexPrefix(txnID [16]byte) []byte {
	return []byte(fmt.Sprintf("ri|%s|", hex.EncodeToString(txnID[:])))
}

func makeVersionedKey(key []byte, writeHT uint64) []byte {
	return []byte(fmt.Sprintf("%s|v|%020d", key, writeHT))
}

func versionPrefix(key []byte) []byte {
	return []byte(fmt.Sprintf("%s|v|", key))
}

func parseVersionedHT(k []byte) (uint64, bool) {
	parts := strings.Split(string(k), "|v|")
	if len(parts) != 2 {
		return 0, false
	}
	ht, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return ht, true
}
