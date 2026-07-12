// Package tablet manages the lifecycle of tablets hosted by a tablet
// server: Manager tracks each tablet's Meta and State through creation,
// splitting, deletion, remote bootstrap, and ownership transfer, persisting
// every transition via a MetaStore before applying it in memory so crashes
// recover to a consistent state. It is consumed by v2/server (the tablet
// server runtime) and references v2/engine/partition to register the
// tablet partitions produced by a split.
// This is scaffold-only; behavior is unimplemented.
package tablet
