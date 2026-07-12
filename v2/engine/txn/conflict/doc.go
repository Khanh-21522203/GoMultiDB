// Package conflict implements intent conflict resolution for the
// distributed transaction manager: Resolver scans the intents database for
// keys a transaction wants to touch, resolves each conflicting intent's
// owning-transaction status via a StatusSource, and applies a decision
// matrix — ignore aborted intents, require a restart against newer
// committed intents, and either abort a lower-priority pending transaction
// or signal the caller to wait. It is consumed by v2/engine/txn as part of
// Manager's write path.
// This is scaffold-only; behavior is unimplemented.
package conflict
