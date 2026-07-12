// Package platform provides low-level node platform services: the
// canonical on-disk directory layout for data and WAL directories
// (FSManager) and hierarchical memory-usage tracking with a hard limit
// (MemTracker). It is consumed by the v2/server node runtimes (master and
// tablet server) when initializing local storage layout and admission
// controlling memory usage.
// This is scaffold-only; behavior is unimplemented.
package platform
