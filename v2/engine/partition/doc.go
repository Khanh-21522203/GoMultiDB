// Package partition maintains the mapping from row keys to owning tablets:
// Map holds a sorted, non-overlapping set of TabletPartition bounds and
// resolves point and range lookups against it, while
// CreateInitialPartitions derives an initial tablet layout from a set of
// split points. It is consumed by v2/engine/tablet, which registers tablet
// splits into a Map, and by v2/server/master/catalog for table creation.
// This is scaffold-only; behavior is unimplemented.
package partition
