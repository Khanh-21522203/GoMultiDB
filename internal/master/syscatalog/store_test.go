package syscatalog_test

import (
	"context"
	"testing"

	"GoMultiDB/internal/common/ids"
	"GoMultiDB/internal/master/catalog"
	"GoMultiDB/internal/master/syscatalog"
	"GoMultiDB/internal/storage/rocks"
)

func newStore() *syscatalog.SysCatalogStore {
	return syscatalog.NewSysCatalogStore(rocks.NewMemoryStore())
}

func TestSysCatalogStoreApplyAndLoad(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	table := catalog.TableInfo{
		TableID:     "table-1",
		NamespaceID: "ns",
		Name:        "users",
		State:       catalog.TableRunning,
		Version:     1,
	}
	if err := s.Apply(ctx, catalog.CatalogMutation{
		RequestID:   "req-1",
		UpsertTable: []catalog.TableInfo{table},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	snap, err := s.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	got, ok := snap.Tables["table-1"]
	if !ok {
		t.Fatalf("table-1 not in snapshot")
	}
	if got.Name != "users" {
		t.Fatalf("name: want users, got %s", got.Name)
	}
}

func TestSysCatalogStoreApplyTablet(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	ti := catalog.TabletInfo{
		TabletID: ids.TabletID("tablet-a"),
		TableID:  "table-1",
		State:    catalog.TabletRunning,
	}
	if err := s.Apply(ctx, catalog.CatalogMutation{
		UpsertTablet: []catalog.TabletInfo{ti},
	}); err != nil {
		t.Fatalf("Apply tablet: %v", err)
	}

	snap, err := s.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	got, ok := snap.Tablets[ids.TabletID("tablet-a")]
	if !ok {
		t.Fatalf("tablet-a not in snapshot")
	}
	if got.State != catalog.TabletRunning {
		t.Fatalf("state: want Running, got %v", got.State)
	}
}

func TestSysCatalogStoreIdempotency(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	table := catalog.TableInfo{
		TableID: "table-idem",
		Name:    "idem",
		State:   catalog.TableRunning,
	}
	// Apply twice with same request ID.
	for i := 0; i < 2; i++ {
		if err := s.Apply(ctx, catalog.CatalogMutation{
			RequestID:          "req-idem",
			RequestKind:        "create_table",
			RequestFingerprint: "ns/idem",
			RequestValue:       "table-idem",
			UpsertTable:        []catalog.TableInfo{table},
		}); err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
	}

	seen, err := s.SeenRequest(ctx, "req-idem")
	if err != nil {
		t.Fatalf("SeenRequest: %v", err)
	}
	if !seen {
		t.Fatalf("expected request to be recorded in reqlog")
	}
	v, ok, err := s.RequestValue(ctx, "req-idem")
	if err != nil {
		t.Fatalf("RequestValue: %v", err)
	}
	if !ok || string(v) != "create_table|ns/idem|table-idem" {
		t.Fatalf("unexpected reqlog payload: ok=%v value=%q", ok, string(v))
	}

	notSeen, err := s.SeenRequest(ctx, "req-never")
	if err != nil {
		t.Fatalf("SeenRequest (unknown): %v", err)
	}
	if notSeen {
		t.Fatalf("unexpected reqlog entry for unknown request")
	}
}

func TestSysCatalogStoreMultipleTables(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	for i := 0; i < 5; i++ {
		tid := ids.TableID(ids.TableID("table-" + string(rune('a'+i))))
		if err := s.Apply(ctx, catalog.CatalogMutation{
			UpsertTable: []catalog.TableInfo{
				{TableID: tid, NamespaceID: "ns", Name: string(rune('a' + i)), State: catalog.TableRunning},
			},
		}); err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
	}

	snap, err := s.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if len(snap.Tables) != 5 {
		t.Fatalf("expected 5 tables, got %d", len(snap.Tables))
	}
}

func TestSysCatalogStoreUpdate(t *testing.T) {
	ctx := context.Background()
	s := newStore()

	table := catalog.TableInfo{
		TableID: "table-upd",
		Name:    "upd",
		State:   catalog.TablePreparing,
		Version: 1,
	}
	if err := s.Apply(ctx, catalog.CatalogMutation{UpsertTable: []catalog.TableInfo{table}}); err != nil {
		t.Fatalf("Apply initial: %v", err)
	}

	table.State = catalog.TableRunning
	table.Version = 2
	if err := s.Apply(ctx, catalog.CatalogMutation{UpsertTable: []catalog.TableInfo{table}}); err != nil {
		t.Fatalf("Apply update: %v", err)
	}

	snap, err := s.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	got := snap.Tables["table-upd"]
	if got.State != catalog.TableRunning || got.Version != 2 {
		t.Fatalf("expected Running/v2, got state=%v version=%d", got.State, got.Version)
	}
}
