package partition

import (
	"sync"

	errors "GoMultiDB/v2/contracts/errors"
)

// PartitionSchema describes how a table's rows are distributed across
// tablets: which columns feed the hash and range components of the key,
// and how many hash buckets to use.
type PartitionSchema struct {
	HashColumns  []int
	RangeColumns []int
	NumBuckets   int
}

// PartitionBound is a tablet's row-key range: [StartKey, EndKey).
type PartitionBound struct {
	StartKey []byte // inclusive
	EndKey   []byte // exclusive; empty means +inf
}

// TabletState is a tablet partition's lifecycle state as seen by the
// partition map.
type TabletState string

// Tablet partition states.
const (
	TabletStateRunning TabletState = "RUNNING"
	TabletStateSplit   TabletState = "SPLIT"
)

// TabletPartition associates a tablet ID with its key-range bound and
// current state.
type TabletPartition struct {
	TabletID string
	Bound    PartitionBound
	State    TabletState
}

// PartitionMap resolves row keys to owning tablets and records tablet
// splits.
type PartitionMap interface {
	FindTablet(key []byte) (string, error)
	ListOverlapping(start, end []byte) ([]TabletPartition, error)
	RegisterTabletSplit(parent string, left, right TabletPartition) error
}

// Map is the in-memory reference implementation of PartitionMap: a sorted,
// non-overlapping set of TabletPartition bounds.
//
// Scaffold stub: all behavior-bearing methods return
// errors.ErrNotImplemented.
type Map struct {
	mu      sync.RWMutex
	tablets []TabletPartition
	byID    map[string]int
}

var _ PartitionMap = (*Map)(nil)

// NewMap returns a Map seeded with tablets, validated to be sorted and
// non-overlapping.
func NewMap(tablets []TabletPartition) (*Map, error) {
	return &Map{}, nil
}

// CreateInitialPartitions derives a sorted, contiguous set of
// TabletPartitions from splitPoints.
//
// Not yet implemented in this scaffold.
func CreateInitialPartitions(schema PartitionSchema, splitPoints [][]byte) ([]TabletPartition, error) {
	return nil, errors.ErrNotImplemented
}

// FindTablet returns the ID of the running tablet whose bound contains key.
//
// Not yet implemented in this scaffold.
func (m *Map) FindTablet(key []byte) (string, error) {
	return "", errors.ErrNotImplemented
}

// ListOverlapping returns every tablet partition overlapping [start, end).
//
// Not yet implemented in this scaffold.
func (m *Map) ListOverlapping(start, end []byte) ([]TabletPartition, error) {
	return nil, errors.ErrNotImplemented
}

// RegisterTabletSplit replaces parent with left and right, validating that
// their bounds exactly tile parent's range.
//
// Not yet implemented in this scaffold.
func (m *Map) RegisterTabletSplit(parent string, left, right TabletPartition) error {
	return errors.ErrNotImplemented
}
