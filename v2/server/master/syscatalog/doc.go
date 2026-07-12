// Package syscatalog provides a durable master/catalog.CatalogStore
// implementation: SysCatalogStore persists tables and tablets as
// individually-keyed JSON records in an infra/storage/rocks.Store, plus a
// request-ID log used for idempotent mutation replay detection. It is
// consumed by v2/server (the master runtime, which wires it in as the
// catalog.Manager's backing store) and depends on master/catalog for the
// mutation and snapshot shapes it persists and contracts/ids for entity
// identifiers.
// This is scaffold-only; behavior is unimplemented.
package syscatalog
