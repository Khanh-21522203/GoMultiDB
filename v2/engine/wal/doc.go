// Package wal provides Log, an append-only, segment-rotating write-ahead
// log keyed by contracts/types.OpID. Entries are appended in order and
// indexed by operation ID so a tablet can replay or seek into its history
// after a crash or during remote bootstrap. It is consumed by
// v2/engine/tablet and v2/engine/docdb for durable operation logging ahead
// of storage application.
// This is scaffold-only; behavior is unimplemented.
package wal
