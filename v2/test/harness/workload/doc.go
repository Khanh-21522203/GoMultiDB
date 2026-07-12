// Package workload provides a configurable workload driver for load and
// correctness testing: RunWorkload drives a mix of reads and writes,
// described by a WorkloadSpec, against a caller-supplied WorkloadTarget at
// a target rate and concurrency, and reports throughput and latency
// percentiles in a WorkloadResult. It is consumed by v2/test/integration
// and v2/test/stress for generating load against a running cluster.
// This is scaffold-only; behavior is unimplemented.
package workload
