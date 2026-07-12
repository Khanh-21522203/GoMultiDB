// Package xcluster implements GoMultiDB v2's cross-cluster apply loop:
// Loop consumes cdc.Event batches, deduplicates them against a checkpoint
// store, applies them through a pluggable Applier with retry, and reports
// throughput, retry, and failure statistics. It references
// replication/cdc for the Event, Checkpoint, and LagSnapshot types it
// operates on. It is consumed by v2/replication/controlplane, whose
// Scheduler drives Loop.ApplyBatch from polled CDC events.
// This is scaffold-only; behavior is unimplemented.
package xcluster
