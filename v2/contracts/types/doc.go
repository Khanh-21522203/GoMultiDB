// Package types declares the shared wire and domain value types used across
// GoMultiDB v2 — request envelopes, hybrid-clock timestamps, and
// replicated-operation identifiers. These are plain value types with no
// behavior beyond deriving new values from their own fields; subsystems in
// engine, server, and gateway reference them.
// This is scaffold-only; behavior is unimplemented.
package types
