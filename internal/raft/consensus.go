package raft

import (
	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/common/types"
	"GoMultiDB/internal/wal"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	Learner
)

type ReplicaState struct {
	CurrentTerm      uint64
	VotedFor         string
	Role             Role
	LeaderID         string
	CommittedOpID    types.OpID
	LastAppliedOpID  types.OpID
	LastReceivedOpID types.OpID
	LeaderLeaseUntil time.Time
	ConfigVersion    uint64
}

type Config struct {
	NodeID              string
	LeaderLeaseDuration time.Duration
	ElectionTimeoutMin  time.Duration
	ElectionTimeoutMax  time.Duration
	ElectionTickCh      <-chan struct{}
}

type VoteRequest struct {
	Term        uint64
	CandidateID string
	LastLogOpID types.OpID
	RequestID   string
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogOpID  types.OpID
	Entries      []wal.Entry
	LeaderCommit types.OpID
}

type AppendEntriesResponse struct {
	Term           uint64
	Success        bool
	LastReceived   types.OpID
	NeedsBootstrap bool // true when follower's log is behind leader's GC boundary
}

type Consensus struct {
	cfg              Config
	wal              *wal.Log
	meta             MetadataStore
	queue            *ReplicationQueue
	mu               sync.RWMutex
	state            ReplicaState
	peers            map[string]struct{} // known peer node IDs (excludes self)
	configInProgress bool                // true while a ChangeConfig is in-flight
	bootstrapBoundary types.OpID         // log entries below this may require remote bootstrap
	stopCh            chan struct{}
	once              sync.Once
	rng               *rand.Rand
	tickerWg          sync.WaitGroup
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

	peers := make(map[string]struct{}, len(meta.Peers))
	for _, p := range meta.Peers {
		peers[p] = struct{}{}
	}

	c := &Consensus{
		cfg:   cfg,
		wal:   walLog,
		meta:  metaStore,
		queue: NewReplicationQueue(),
		state: ReplicaState{
			CurrentTerm:   meta.CurrentTerm,
			VotedFor:      meta.VotedFor,
			Role:          Follower,
			ConfigVersion: meta.ConfigVersion,
		},
		peers:  peers,
		stopCh: make(chan struct{}),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
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

	needsBootstrap := false
	if !c.bootstrapBoundary.IsZero() && c.state.LastReceivedOpID.Less(c.bootstrapBoundary) {
		needsBootstrap = true
	}

	return AppendEntriesResponse{Term: c.state.CurrentTerm, Success: true, LastReceived: c.state.LastReceivedOpID, NeedsBootstrap: needsBootstrap}, nil
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

// ── Leader lease reads ────────────────────────────────────────────────────────

// GetLeaderLeaseStatus returns whether the node believes it holds a valid leader
// lease and when the lease expires. Returns (false, zero) if not a leader.
func (c *Consensus) GetLeaderLeaseStatus() (valid bool, expiresAt time.Time) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state.Role != Leader {
		return false, time.Time{}
	}
	expiresAt = c.state.LeaderLeaseUntil
	return time.Now().Before(expiresAt), expiresAt
}

// ReadAtLease returns nil if the leader lease is currently valid, allowing
// a linearizable read without a round-trip. Returns ErrLeaseExpired otherwise.
func (c *Consensus) ReadAtLease(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	valid, _ := c.GetLeaderLeaseStatus()
	if !valid {
		return dberrors.New(dberrors.ErrLeaseExpired, "leader lease expired or not leader", true, nil)
	}
	return nil
}

// ── Pre-vote ──────────────────────────────────────────────────────────────────

// PreVoteRequest is sent by a candidate before incrementing its term.
// A majority of pre-votes are required before a real election begins.
type PreVoteRequest struct {
	// NextTerm is the term the candidate would use in a real election (current+1).
	NextTerm    uint64
	CandidateID string
	LastLogOpID types.OpID
	RequestID   string
}

// PreVoteResponse carries a node's response to a PreVoteRequest.
type PreVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// HandlePreVote processes a pre-vote request without mutating persistent state
// (pre-vote does not update CurrentTerm or VotedFor).
//
// A pre-vote is granted when:
//   - This node has not heard from a valid leader recently (lease expired), and
//   - The candidate's log is at least as up-to-date as ours.
func (c *Consensus) HandlePreVote(_ context.Context, req PreVoteRequest) (PreVoteResponse, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Reject if we know of an active leader (our follower lease is still valid).
	// This prevents isolated nodes from disrupting a functioning cluster.
	followerLeaseValid := c.state.Role == Follower &&
		!c.state.LeaderLeaseUntil.IsZero() &&
		time.Now().Before(c.state.LeaderLeaseUntil)
	leaderIsUs := c.state.Role == Leader && time.Now().Before(c.state.LeaderLeaseUntil)

	if followerLeaseValid || leaderIsUs {
		return PreVoteResponse{Term: c.state.CurrentTerm, VoteGranted: false}, nil
	}

	// Candidate's log must be at least as up-to-date as ours.
	logUpToDate := c.state.LastReceivedOpID.Less(req.LastLogOpID) ||
		c.state.LastReceivedOpID.Equal(req.LastLogOpID)
	if !logUpToDate {
		return PreVoteResponse{Term: c.state.CurrentTerm, VoteGranted: false}, nil
	}

	return PreVoteResponse{Term: c.state.CurrentTerm, VoteGranted: true}, nil
}

// ── Dynamic membership change ─────────────────────────────────────────────────

// ConfigChangeType enumerates supported membership operations.
type ConfigChangeType int

const (
	AddPeer    ConfigChangeType = iota // add a new voting peer
	RemovePeer                         // remove an existing peer
)

// ConfigChangeOp describes a single membership change.
type ConfigChangeOp struct {
	Type   ConfigChangeType
	PeerID string
}

// ChangeConfig applies a single membership change (add or remove peer).
// Only one config change may be in-flight at a time; subsequent calls return
// ErrConflict until the first completes.
//
// The change is persisted in Metadata before being applied in-memory so that
// restarts see the updated peer list.
func (c *Consensus) ChangeConfig(_ context.Context, op ConfigChangeOp) error {
	if op.PeerID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "peer id is required", false, nil)
	}
	if op.PeerID == c.cfg.NodeID {
		return dberrors.New(dberrors.ErrInvalidArgument, "cannot change config for self", false, nil)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state.Role != Leader {
		return dberrors.New(dberrors.ErrNotLeader, "config change requires leader", true, nil)
	}
	if c.configInProgress {
		return dberrors.New(dberrors.ErrConflict, "config change already in progress", true, nil)
	}

	switch op.Type {
	case AddPeer:
		if _, exists := c.peers[op.PeerID]; exists {
			return dberrors.New(dberrors.ErrConflict, "peer already in config", false, nil)
		}
	case RemovePeer:
		if _, exists := c.peers[op.PeerID]; !exists {
			return dberrors.New(dberrors.ErrInvalidArgument, "peer not in config", false, nil)
		}
	default:
		return dberrors.New(dberrors.ErrInvalidArgument, "unknown config change type", false, nil)
	}

	c.configInProgress = true
	defer func() { c.configInProgress = false }()

	// Apply the change to an in-memory copy of peers.
	newPeers := make(map[string]struct{}, len(c.peers))
	for k := range c.peers {
		newPeers[k] = struct{}{}
	}
	switch op.Type {
	case AddPeer:
		newPeers[op.PeerID] = struct{}{}
	case RemovePeer:
		delete(newPeers, op.PeerID)
	}

	// Persist before applying in-memory.
	peerList := make([]string, 0, len(newPeers))
	for k := range newPeers {
		peerList = append(peerList, k)
	}
	c.state.ConfigVersion++
	if err := c.meta.Save(Metadata{
		CurrentTerm:   c.state.CurrentTerm,
		VotedFor:      c.state.VotedFor,
		ConfigVersion: c.state.ConfigVersion,
		Peers:         peerList,
	}); err != nil {
		c.state.ConfigVersion-- // rollback counter
		return fmt.Errorf("raft: persist config change: %w", err)
	}
	c.peers = newPeers
	return nil
}

// Peers returns a snapshot of the current peer set (excludes self).
func (c *Consensus) Peers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.peers))
	for k := range c.peers {
		out = append(out, k)
	}
	return out
}

// SetBootstrapBoundary sets the log GC boundary used to indicate that a
// follower requires remote bootstrap when it is too far behind.
func (c *Consensus) SetBootstrapBoundary(boundary types.OpID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bootstrapBoundary = boundary
}

// ── internal ─────────────────────────────────────────────────────────────────

func (c *Consensus) persistMetadataLocked() error {
	peers := make([]string, 0, len(c.peers))
	for k := range c.peers {
		peers = append(peers, k)
	}
	return c.meta.Save(Metadata{
		CurrentTerm:   c.state.CurrentTerm,
		VotedFor:      c.state.VotedFor,
		ConfigVersion: c.state.ConfigVersion,
		Peers:         peers,
	})
}
