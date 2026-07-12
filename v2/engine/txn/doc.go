// Package txn implements the coordinator-side distributed transaction
// manager: Manager tracks each transaction's lifecycle (Pending →
// Committing/Aborting → Committed/Aborted) and, on commit or abort, drives
// an IntentApplier to resolve the intents staged in each involved tablet's
// v2/engine/docdb.Engine. It is consumed by v2/server (the tablet server
// and master runtimes) and coordinates with v2/engine/txn/conflict and
// v2/engine/txn/waitq for conflict detection and deadlock resolution.
// This is scaffold-only; behavior is unimplemented.
package txn
