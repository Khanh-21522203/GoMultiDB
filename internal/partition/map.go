package partition

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	dberrors "GoMultiDB/internal/common/errors"
)

type PartitionSchema struct {
	HashColumns  []int
	RangeColumns []int
	NumBuckets   int
}

type PartitionBound struct {
	StartKey []byte // inclusive
	EndKey   []byte // exclusive; empty means +inf
}

type TabletState string

const (
	TabletStateRunning TabletState = "RUNNING"
	TabletStateSplit   TabletState = "SPLIT"
)

type TabletPartition struct {
	TabletID string
	Bound    PartitionBound
	State    TabletState
}

type PartitionMap interface {
	FindTablet(key []byte) (string, error)
	ListOverlapping(start, end []byte) ([]TabletPartition, error)
	RegisterTabletSplit(parent string, left, right TabletPartition) error
}

type Map struct {
	mu      sync.RWMutex
	tablets []TabletPartition
	byID    map[string]int
}

func NewMap(tablets []TabletPartition) (*Map, error) {
	m := &Map{byID: make(map[string]int)}
	if err := m.setTablets(tablets); err != nil {
		return nil, err
	}
	return m, nil
}

func CreateInitialPartitions(_ PartitionSchema, splitPoints [][]byte) ([]TabletPartition, error) {
	bounds := make([]PartitionBound, 0, len(splitPoints)+1)
	start := []byte{}
	for _, p := range splitPoints {
		if len(p) == 0 {
			return nil, dberrors.New(dberrors.ErrInvalidArgument, "split points cannot be empty", false, nil)
		}
		if len(start) > 0 && bytes.Compare(start, p) >= 0 {
			return nil, dberrors.New(dberrors.ErrInvalidArgument, "split points must be sorted and unique", false, nil)
		}
		bounds = append(bounds, PartitionBound{StartKey: clone(start), EndKey: clone(p)})
		start = clone(p)
	}
	bounds = append(bounds, PartitionBound{StartKey: clone(start), EndKey: nil})

	out := make([]TabletPartition, 0, len(bounds))
	for i, b := range bounds {
		out = append(out, TabletPartition{
			TabletID: fmt.Sprintf("tablet-%d", i+1),
			Bound:    b,
			State:    TabletStateRunning,
		})
	}
	return out, nil
}

func (m *Map) FindTablet(key []byte) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.tablets) == 0 {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "empty partition map", false, nil)
	}
	i := sort.Search(len(m.tablets), func(i int) bool {
		e := m.tablets[i].Bound.EndKey
		return len(e) == 0 || bytes.Compare(e, key) > 0
	})
	if i >= len(m.tablets) {
		return "", dberrors.New(dberrors.ErrInvalidArgument, "no route for key", true, nil)
	}
	for ; i < len(m.tablets); i++ {
		t := m.tablets[i]
		if bytes.Compare(key, t.Bound.StartKey) < 0 {
			return "", dberrors.New(dberrors.ErrInvalidArgument, "stale partition map", true, nil)
		}
		if t.State != TabletStateRunning {
			continue
		}
		if len(t.Bound.EndKey) == 0 || bytes.Compare(key, t.Bound.EndKey) < 0 {
			return t.TabletID, nil
		}
	}
	return "", dberrors.New(dberrors.ErrInvalidArgument, "no route for key", true, nil)
}

func (m *Map) ListOverlapping(start, end []byte) ([]TabletPartition, error) {
	if len(end) > 0 && bytes.Compare(start, end) >= 0 {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid range", false, nil)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]TabletPartition, 0)
	for _, t := range m.tablets {
		if overlaps(t.Bound, start, end) {
			out = append(out, cloneTablet(t))
		}
	}
	return out, nil
}

func (m *Map) RegisterTabletSplit(parent string, left, right TabletPartition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx, ok := m.byID[parent]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "parent tablet not found", false, nil)
	}
	p := m.tablets[idx]
	if p.State != TabletStateRunning {
		return dberrors.New(dberrors.ErrConflict, "parent not in running state", true, nil)
	}
	if !isChildRangeValid(p.Bound, left.Bound, right.Bound) {
		return dberrors.New(dberrors.ErrInvalidArgument, "invalid child partition bounds", false, nil)
	}
	if _, exists := m.byID[left.TabletID]; exists {
		return dberrors.New(dberrors.ErrConflict, "left tablet id exists", false, nil)
	}
	if _, exists := m.byID[right.TabletID]; exists {
		return dberrors.New(dberrors.ErrConflict, "right tablet id exists", false, nil)
	}

	left.State = TabletStateRunning
	right.State = TabletStateRunning

	next := make([]TabletPartition, 0, len(m.tablets)+1)
	next = append(next, m.tablets[:idx]...)
	next = append(next, left, right)
	next = append(next, m.tablets[idx+1:]...)
	if err := m.setTablets(next); err != nil {
		return err
	}
	return nil
}

func (m *Map) setTablets(tablets []TabletPartition) error {
	for i := range tablets {
		tablets[i] = cloneTablet(tablets[i])
	}
	sort.Slice(tablets, func(i, j int) bool {
		return bytes.Compare(tablets[i].Bound.StartKey, tablets[j].Bound.StartKey) < 0
	})
	if err := validateNoOverlap(tablets); err != nil {
		return err
	}
	m.tablets = tablets
	m.byID = make(map[string]int, len(tablets))
	for i, t := range tablets {
		if t.TabletID == "" {
			return dberrors.New(dberrors.ErrInvalidArgument, "tablet id required", false, nil)
		}
		if _, exists := m.byID[t.TabletID]; exists {
			return dberrors.New(dberrors.ErrInvalidArgument, "duplicate tablet id", false, nil)
		}
		m.byID[t.TabletID] = i
	}
	return nil
}

func validateNoOverlap(tablets []TabletPartition) error {
	for i := 1; i < len(tablets); i++ {
		prev := tablets[i-1].Bound
		cur := tablets[i].Bound
		if len(prev.EndKey) == 0 {
			return dberrors.New(dberrors.ErrInvalidArgument, "open-ended partition must be last", false, nil)
		}
		if bytes.Compare(prev.EndKey, cur.StartKey) != 0 {
			return dberrors.New(dberrors.ErrInvalidArgument, "partition gap or overlap detected", false, nil)
		}
	}
	return nil
}

func isChildRangeValid(parent, left, right PartitionBound) bool {
	if bytes.Compare(left.StartKey, parent.StartKey) != 0 {
		return false
	}
	if len(parent.EndKey) == 0 {
		if len(right.EndKey) != 0 {
			return false
		}
	} else if bytes.Compare(right.EndKey, parent.EndKey) != 0 {
		return false
	}
	if len(left.EndKey) == 0 || bytes.Compare(left.EndKey, right.StartKey) != 0 {
		return false
	}
	if bytes.Compare(left.StartKey, left.EndKey) >= 0 {
		return false
	}
	if len(right.EndKey) > 0 && bytes.Compare(right.StartKey, right.EndKey) >= 0 {
		return false
	}
	return true
}

func overlaps(bound PartitionBound, start, end []byte) bool {
	if len(end) > 0 && bytes.Compare(bound.StartKey, end) >= 0 {
		return false
	}
	if len(bound.EndKey) > 0 && bytes.Compare(bound.EndKey, start) <= 0 {
		return false
	}
	return true
}

func cloneTablet(t TabletPartition) TabletPartition {
	t.Bound.StartKey = clone(t.Bound.StartKey)
	t.Bound.EndKey = clone(t.Bound.EndKey)
	return t
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
