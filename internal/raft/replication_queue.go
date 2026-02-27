package raft

import (
	"sync"

	"GoMultiDB/internal/wal"
)

type ReplicationQueue struct {
	mu      sync.Mutex
	entries []wal.Entry
}

func NewReplicationQueue() *ReplicationQueue {
	return &ReplicationQueue{}
}

func (q *ReplicationQueue) Enqueue(entries []wal.Entry) {
	if len(entries) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.entries = append(q.entries, entries...)
}

func (q *ReplicationQueue) Drain(max int) []wal.Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return nil
	}
	if max <= 0 || max > len(q.entries) {
		max = len(q.entries)
	}
	out := make([]wal.Entry, max)
	copy(out, q.entries[:max])
	q.entries = q.entries[max:]
	return out
}

func (q *ReplicationQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}
