// Package cdc implements GoMultiDB v2's change-data-capture subsystem: an
// in-memory Store of per-stream, per-tablet event logs and checkpoints, a
// Service exposing stream lifecycle and change-polling operations over
// that store, checkpoint persistence (FileCheckpointStore) and remapping
// across tablet splits (SplitRemapper), an RPC-based Poller and
// RPCProducer pair for pulling changes from a remote cluster, and a
// MetricsRegistry that publishes stream lag and throughput into
// replication/observability. It references contracts/ids and
// contracts/types for identifiers and RPC envelopes, infra/rpc for its
// remote producer transport, and replication/observability for metrics.
// It is consumed by v2/replication/xcluster, which applies polled events
// to a target cluster, and v2/replication/controlplane, which schedules
// and supervises streams.
// This is scaffold-only; behavior is unimplemented.
package cdc
