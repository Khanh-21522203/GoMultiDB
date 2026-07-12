package catalog

import (
	"context"
	"sync"

	errors "GoMultiDB/v2/contracts/errors"
)

// TransferState is the ownership-transfer state of a tablet's primary
// replica.
type TransferState string

// Tablet ownership transfer states.
const (
	TransferStateNone     TransferState = "NONE"
	TransferStatePrepared TransferState = "TRANSFER_PREPARED"
)

// TabletReplicaStatus describes one replica's reporting state for a
// tablet.
type TabletReplicaStatus struct {
	TSUUID    string
	LastSeqNo uint64
}

// TabletPlacementView is the reconciled placement view for a single
// tablet: its live replicas, primary owner, and any in-flight ownership
// transfer.
type TabletPlacementView struct {
	TabletID             string
	Replicas             map[string]TabletReplicaStatus
	PrimaryTSUUID        string
	Tombstoned           bool
	LastUpdated          uint64
	OwnerEpoch           uint64
	TransferState        TransferState
	TransferEpoch        uint64
	PendingPrimaryTSUUID string
}

// MemoryReconcileSink is an in-memory ReconcileSink that maintains a
// TabletPlacementView per tablet, derived from applied
// TabletReportDeltas, and supports primary ownership transfer.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented, except ApplyTabletReport which is a no-op.
type MemoryReconcileSink struct {
	mu      sync.RWMutex
	tablets map[string]TabletPlacementView
}

var _ ReconcileSink = (*MemoryReconcileSink)(nil)

// NewMemoryReconcileSink returns an empty MemoryReconcileSink.
func NewMemoryReconcileSink() *MemoryReconcileSink {
	return &MemoryReconcileSink{}
}

// ApplyTabletReport applies delta to the sink's placement view.
//
// Not yet implemented in this scaffold.
func (s *MemoryReconcileSink) ApplyTabletReport(_ context.Context, delta TabletReportDelta) error {
	return errors.ErrNotImplemented
}

// GetTablet returns the TabletPlacementView for tabletID.
//
// Not yet implemented in this scaffold.
func (s *MemoryReconcileSink) GetTablet(tabletID string) (TabletPlacementView, bool) {
	panic("not implemented")
}

// ListTablets returns the placement views of all known tablets.
//
// Not yet implemented in this scaffold.
func (s *MemoryReconcileSink) ListTablets() []TabletPlacementView {
	panic("not implemented")
}

// PrepareOwnershipTransfer marks tabletID as transferring its primary
// ownership to targetPrimary, recording a new TransferEpoch.
//
// Not yet implemented in this scaffold.
func (s *MemoryReconcileSink) PrepareOwnershipTransfer(_ context.Context, tabletID, targetPrimary string) (TabletPlacementView, error) {
	return TabletPlacementView{}, errors.ErrNotImplemented
}

// CommitOwnershipTransfer completes a previously prepared ownership
// transfer for tabletID, verifying expectedTransferEpoch when nonzero.
//
// Not yet implemented in this scaffold.
func (s *MemoryReconcileSink) CommitOwnershipTransfer(_ context.Context, tabletID string, expectedTransferEpoch uint64) (TabletPlacementView, error) {
	return TabletPlacementView{}, errors.ErrNotImplemented
}
