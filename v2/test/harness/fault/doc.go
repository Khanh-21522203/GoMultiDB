// Package fault provides fault injection primitives for distributed system
// testing: FaultInjector maintains a set of active FaultActions
// (partitions, kills, delays, disk-full, and clock-skew faults) that RPC
// and storage interceptors elsewhere in the v2 module tree consult via
// IsPartitioned, DelayFor, IsKilled, IsDiskFull, and ClockSkewFor. It is
// consumed by v2/test/integration and v2/test/stress scenarios that
// exercise cluster resilience under injected faults.
// This is scaffold-only; behavior is unimplemented.
package fault
