// Package pggate bridges PostgreSQL-wire sessions to GoMultiDB v2's
// distributed transaction and tablet-dispatch layers. Manager tracks
// per-session state (table cache, pending writes, and a TxnHandle with
// savepoints) and drives reads, writes, and transactions through the
// PartitionResolver, TabletDispatcher, and TxnCoordinator interfaces —
// the seams through which tablet RPC and v2/engine/txn are wired in by
// v2/server. RetryStats and ExecResponse surface conflict/retry
// telemetry, and a contracts/idempotency.Store deduplicates retried
// ExecWrite/ExecRead calls keyed by contracts/ids.RequestID.
// This is scaffold-only; behavior is unimplemented.
package pggate
