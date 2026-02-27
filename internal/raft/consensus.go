package raft

import (
	"context"

	"fmt"

	"math/rand"

	"sync"

	"time"

	dberrors "GoMultiDB/internal/common/errors"

	"GoMultiDB/internal/common/types"

	"GoMultiDB/internal/wal"
)

type Role int

const (
	Follower Role = iota

	Candidate

	Leader

	Learner
)

type ReplicaState struct {
	CurrentTerm uint64

	VotedFor string

	Role Role

	LeaderID string

	CommittedOpID types.OpID

	LastAppliedOpID types.OpID

	LastReceivedOpID types.OpID

	LeaderLeaseUntil time.Time

	ConfigVersion uint64
}

type Config struct {
	NodeID string

	LeaderLeaseDuration time.Duration

	ElectionTimeoutMin time.Duration

	ElectionTimeoutMax time.Duration

	ElectionTickCh <-chan struct{}
}

type VoteRequest struct {
	Term uint64

	CandidateID string

	LastLogOpID types.OpID

	RequestID string
}

type VoteResponse struct {
	Term uint64

	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term uint64

	LeaderID string

	PrevLogOpID types.OpID

	Entries []wal.Entry

	LeaderCommit types.OpID
}

type AppendEntriesResponse struct {
	Term uint64

	Success bool

	LastReceived types.OpID
}

type Consensus struct {
	cfg Config

	wal *wal.Log

	meta MetadataStore

	queue *ReplicationQueue

	mu sync.RWMutex

	state ReplicaState

	stopCh chan struct{}

	once sync.Once

	rng *rand.Rand

	tickerWg sync.WaitGroup
}

func NewConsensus(cfg Config, walLog *wal.Log, metaStore MetadataStore) (*Consensus, error) {

	if cfg.NodeID == "" {

		return nil, fmt.Errorf("node id is required")

	}

	if walLog == nil {

		return nil, fmt.Errorf("wal is required")

	}

	if metaStore == nil {

		return nil, fmt.Errorf("metadata store is required")

	}

	if cfg.LeaderLeaseDuration <= 0 {

		cfg.LeaderLeaseDuration = 2 * time.Second

	}

	if cfg.ElectionTimeoutMin <= 0 {

		cfg.ElectionTimeoutMin = 1500 * time.Millisecond

	}

	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {

		cfg.ElectionTimeoutMax = 3000 * time.Millisecond

	}

	meta, err := metaStore.Load()

	if err != nil {

		return nil, fmt.Errorf("load consensus metadata: %w", err)

	}

	if meta.CurrentTerm == 0 {

		meta.CurrentTerm = 1

	}

	c := &Consensus{

		cfg: cfg,

		wal: walLog,

		meta: metaStore,

		queue: NewReplicationQueue(),

		state: ReplicaState{

			CurrentTerm: meta.CurrentTerm,

			VotedFor: meta.VotedFor,

			Role: Follower,

			ConfigVersion: meta.ConfigVersion,
		},

		stopCh: make(chan struct{}),

		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	return c, nil

}

func (c *Consensus) Start() {

	c.tickerWg.Add(1)

	go c.runElectionTimer()

}

func (c *Consensus) Stop() {

	c.once.Do(func() {

		close(c.stopCh)

		c.tickerWg.Wait()

	})

}

func (c *Consensus) State() ReplicaState {

	c.mu.RLock()

	defer c.mu.RUnlock()

	return c.state

}

func (c *Consensus) BecomeLeader() {

	c.mu.Lock()

	defer c.mu.Unlock()

	c.state.Role = Leader

	c.state.LeaderID = c.cfg.NodeID

	c.state.LeaderLeaseUntil = time.Now().Add(c.cfg.LeaderLeaseDuration)

}

func (c *Consensus) StepDown(newTerm uint64, leaderID string) {

	c.mu.Lock()

	defer c.mu.Unlock()

	if newTerm > c.state.CurrentTerm {

		c.state.CurrentTerm = newTerm

		_ = c.persistMetadataLocked()

	}

	c.state.Role = Follower

	c.state.LeaderID = leaderID

	c.state.LeaderLeaseUntil = time.Time{}

}

func (c *Consensus) Replicate(ctx context.Context, entries []wal.Entry) error {

	c.mu.RLock()

	st := c.state

	c.mu.RUnlock()

	if st.Role != Leader {

		return dberrors.New(dberrors.ErrNotLeader, "replicate requires leader", true, nil)

	}

	if time.Now().After(st.LeaderLeaseUntil) {

		return dberrors.New(dberrors.ErrTimeout, "leader lease expired", true, nil)

	}

	if err := c.wal.AppendSync(ctx, entries); err != nil {

		return dberrors.New(dberrors.ErrInternal, "append wal", false, err)

	}

	c.queue.Enqueue(entries)

	c.mu.Lock()

	defer c.mu.Unlock()

	if len(entries) > 0 {

		last := entries[len(entries)-1].OpID

		c.state.LastReceivedOpID = last

		c.state.CommittedOpID = last

		c.state.LastAppliedOpID = last

	}

	c.state.LeaderLeaseUntil = time.Now().Add(c.cfg.LeaderLeaseDuration)

	return nil

}

func (c *Consensus) HandleAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {

	c.mu.Lock()

	defer c.mu.Unlock()

	if req.Term < c.state.CurrentTerm {

		return AppendEntriesResponse{Term: c.state.CurrentTerm, Success: false, LastReceived: c.state.LastReceivedOpID}, nil

	}

	if req.Term > c.state.CurrentTerm {

		c.state.CurrentTerm = req.Term

		c.state.VotedFor = ""

		if err := c.persistMetadataLocked(); err != nil {

			return AppendEntriesResponse{}, dberrors.New(dberrors.ErrInternal, "persist term on append entries", false, err)

		}

	}

	c.state.Role = Follower

	c.state.LeaderID = req.LeaderID

	c.state.LeaderLeaseUntil = time.Now().Add(c.cfg.LeaderLeaseDuration)

	if len(req.Entries) > 0 {

		c.mu.Unlock()

		err := c.wal.AppendSync(ctx, req.Entries)

		c.mu.Lock()

		if err != nil {

			return AppendEntriesResponse{Term: c.state.CurrentTerm, Success: false, LastReceived: c.state.LastReceivedOpID}, dberrors.New(dberrors.ErrInternal, "append follower wal", false, err)

		}

		last := req.Entries[len(req.Entries)-1].OpID

		c.state.LastReceivedOpID = last

	}

	if !req.LeaderCommit.IsZero() && (c.state.CommittedOpID.IsZero() || c.state.CommittedOpID.Less(req.LeaderCommit)) {

		c.state.CommittedOpID = req.LeaderCommit

		c.state.LastAppliedOpID = req.LeaderCommit

	}

	return AppendEntriesResponse{Term: c.state.CurrentTerm, Success: true, LastReceived: c.state.LastReceivedOpID}, nil

}

func (c *Consensus) HandleRequestVote(_ context.Context, req VoteRequest) (VoteResponse, error) {

	c.mu.Lock()

	defer c.mu.Unlock()

	if req.Term < c.state.CurrentTerm {

		return VoteResponse{Term: c.state.CurrentTerm, VoteGranted: false}, nil

	}

	if req.Term > c.state.CurrentTerm {

		c.state.CurrentTerm = req.Term

		c.state.VotedFor = ""

		c.state.Role = Follower

		if err := c.persistMetadataLocked(); err != nil {

			return VoteResponse{}, dberrors.New(dberrors.ErrInternal, "persist term on vote", false, err)

		}

	}

	if c.state.VotedFor != "" && c.state.VotedFor != req.CandidateID {

		return VoteResponse{Term: c.state.CurrentTerm, VoteGranted: false}, nil

	}

	if c.state.LastReceivedOpID.Less(req.LastLogOpID) || c.state.LastReceivedOpID.Equal(req.LastLogOpID) {

		c.state.VotedFor = req.CandidateID

		if err := c.persistMetadataLocked(); err != nil {

			return VoteResponse{}, dberrors.New(dberrors.ErrInternal, "persist vote", false, err)

		}

		return VoteResponse{Term: c.state.CurrentTerm, VoteGranted: true}, nil

	}

	return VoteResponse{Term: c.state.CurrentTerm, VoteGranted: false}, nil

}

func (c *Consensus) runElectionTimer() {

	defer c.tickerWg.Done()

	if c.cfg.ElectionTickCh != nil {

		for {

			select {

			case <-c.cfg.ElectionTickCh:

				c.onElectionTimeout()

			case <-c.stopCh:

				return

			}

		}

	}

	for {

		t := c.randomElectionTimeout()

		timer := time.NewTimer(t)

		select {

		case <-timer.C:

			c.onElectionTimeout()

		case <-c.stopCh:

			if !timer.Stop() {

				select {

				case <-timer.C:

				default:

				}

			}

			return

		}

	}

}

func (c *Consensus) onElectionTimeout() {

	c.mu.Lock()

	defer c.mu.Unlock()

	if c.state.Role == Leader {

		return

	}

	c.state.Role = Candidate

	c.state.CurrentTerm++

	c.state.VotedFor = c.cfg.NodeID

	_ = c.persistMetadataLocked()

}

func (c *Consensus) randomElectionTimeout() time.Duration {

	c.mu.RLock()

	min := c.cfg.ElectionTimeoutMin

	max := c.cfg.ElectionTimeoutMax

	c.mu.RUnlock()

	if max <= min {

		return min

	}

	delta := max - min

	n := c.rng.Int63n(int64(delta))

	return min + time.Duration(n)

}

func (c *Consensus) persistMetadataLocked() error {

	return c.meta.Save(Metadata{

		CurrentTerm: c.state.CurrentTerm,

		VotedFor: c.state.VotedFor,

		ConfigVersion: c.state.ConfigVersion,
	})

}
