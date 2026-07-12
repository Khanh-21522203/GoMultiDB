// Package idempotency declares the Store interface and an in-memory
// reference implementation used to detect and reject duplicate client
// requests within a scope, keyed by request id and guarded by a
// caller-supplied fingerprint. Server and gateway modules consult a Store
// before executing a request that must not be applied twice.
// This is scaffold-only; behavior is unimplemented.
package idempotency
