// Package stress provides Harness, a configurable driver for the v2 stress
// test scenarios (steady-state, bursty-with-faults, scale-out, and soak):
// Config and its per-scenario Workload/Thresholds describe the load shape
// and pass/fail bounds for each ScenarioName, EvaluateThresholds grades a
// ScenarioResult against its Thresholds, and BuildSummary/WithRunStats
// assemble a run's Summary, which Harness persists to disk alongside its
// Config as JSON artifacts under tests/stress/artifacts. It is consumed by
// the v2 stress test suites driving sustained and adversarial load against
// a running cluster.
// This is scaffold-only; behavior is unimplemented.
package stress
