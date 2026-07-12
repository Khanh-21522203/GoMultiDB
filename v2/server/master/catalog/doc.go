// Package catalog implements the master's table and tablet metadata store:
// Manager owns the authoritative CatalogSnapshot (tables and tablets)
// behind a pluggable CatalogStore, applying idempotent CreateTable,
// AlterTable, DeleteTable, and CreateTablet mutations and reconciling
// tablet placement reports from tablet servers through a ReconcileSink
// (MemoryReconcileSink). DirectivePlanner compares observed tablet
// placement (TabletPlacementView) against the desired replication factor
// and emits TabletDirective actions for the master to carry out. It is
// consumed by v2/server/master/heartbeat (which drives
// ProcessTabletReport, ApplyTabletReport, and DirectivePlanner from
// tserver heartbeats) and v2/server/master/syscatalog (which implements
// CatalogStore durably).
// This is scaffold-only; behavior is unimplemented.
package catalog
