// Package docdb provides Engine, the per-tablet document storage engine
// built on infra/storage/rocks.Store. It applies non-transactional writes
// directly, stages transactional writes as intents keyed by transaction ID,
// and later applies or removes those intents once the owning transaction
// commits or aborts. It is consumed by v2/engine/tablet and v2/engine/txn
// for tablet-local document storage and intent lifecycle management.
// This is scaffold-only; behavior is unimplemented.
package docdb
