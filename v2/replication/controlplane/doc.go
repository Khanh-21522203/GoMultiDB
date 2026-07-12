// Package controlplane implements GoMultiDB v2's replication control
// plane: Registry tracks the lifecycle of replication Streams and Jobs
// (create, pause, resume, stop) and produces point-in-time Snapshots
// combining checkpoint and lag data, while Scheduler periodically polls
// each running stream's CDC store and drives its xcluster.Loop to apply
// batches within a per-job in-flight cap. It references replication/cdc
// for the change store it schedules polls against and replication/
// xcluster for the apply loop it drives.
// This is scaffold-only; behavior is unimplemented.
package controlplane
