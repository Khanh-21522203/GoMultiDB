// Package rocks provides Store, an ordered key-value abstraction split
// into two logical databases (a regular DB for committed data and an
// intents DB for provisional transaction writes), and MemoryStore, an
// in-memory reference implementation used ahead of a real RocksDB-backed
// implementation. It is consumed by v2/engine/tablet, v2/engine/docdb,
// v2/engine/tablet/snapshot, and v2/server/master/syscatalog for
// tablet-local and catalog persistence.
// This is scaffold-only; behavior is unimplemented.
package rocks
