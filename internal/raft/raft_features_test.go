package raft_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"GoMultiDB/internal/common/types"
	"GoMultiDB/internal/raft"
	"GoMultiDB/internal/wal"
)

// ── leader lease reads ────────────────────────────────────────────────────────

func TestLeaderLeaseStatusNotLeader(t *testing.T) {
	c, l := newConsensusForTest(t, "n-lease-1")
	defer l.Close()
	defer c.Stop()

	valid, exp := c.GetLeaderLeaseStatus()
	if valid {
		t.Fatalf("follower should not have valid lease")
	}
	if !exp.IsZero() {
		t.Fatalf("expiry should be zero for non-leader")
	}
}

func TestLeaderLeaseStatusValid(t *testing.T) {
	c, l := newConsensusForTest(t, "n-lease-2")
	defer l.Close()
	defer c.Stop()

	c.BecomeLeader()
	valid, exp := c.GetLeaderLeaseStatus()
	if !valid {
		t.Fatalf("leader should have valid lease immediately after BecomeLeader")
	}
	if exp.Before(time.Now()) {
		t.Fatalf("lease expiry should be in the future, got %v", exp)
	}
}

func TestReadAtLeaseSucceedsForLeader(t *testing.T) {
	c, l := newConsensusForTest(t, "n-lease-3")
	defer l.Close()
	defer c.Stop()

	c.BecomeLeader()
	if err := c.ReadAtLease(context.Background()); err != nil {
		t.Fatalf("ReadAtLease should succeed for leader in lease: %v", err)
	}
}

func TestReadAtLeaseFailsForFollower(t *testing.T) {
	c, l := newConsensusForTest(t, "n-lease-4")
	defer l.Close()
	defer c.Stop()

	// Follower has no lease.
	if err := c.ReadAtLease(context.Background()); err == nil {
		t.Fatalf("ReadAtLease should fail for follower")
	}
}

func TestReadAtLeaseRespectsContext(t *testing.T) {
	c, l := newConsensusForTest(t, "n-lease-5")
	defer l.Close()
	defer c.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	if err := c.ReadAtLease(ctx); err == nil {
		t.Fatalf("ReadAtLease should fail on cancelled context")
	}
}

// ── pre-vote ──────────────────────────────────────────────────────────────────

func TestHandlePreVoteGrantedWhenLeaseExpired(t *testing.T) {
	c, l := newConsensusForTest(t, "n-pv-1")
	defer l.Close()
	defer c.Stop()

	// Fresh follower with no active leader lease — should grant pre-vote.
	resp, err := c.HandlePreVote(context.Background(), raft.PreVoteRequest{
		NextTerm:    2,
		CandidateID: "cand-1",
		LastLogOpID: types.OpID{Term: 1, Index: 5},
	})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatalf("expected pre-vote granted for follower with no active leader")
	}
}

func TestHandlePreVoteRejectedActiveLeaderLease(t *testing.T) {
	c, l := newConsensusForTest(t, "n-pv-2")
	defer l.Close()
	defer c.Stop()

	// Give the follower a fresh heartbeat (lease refreshed).
	_, err := c.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term:     3,
		LeaderID: "leader-a",
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}

	// Now follower believes leader-a is active — reject pre-vote.
	resp, err := c.HandlePreVote(context.Background(), raft.PreVoteRequest{
		NextTerm:    4,
		CandidateID: "cand-2",
		LastLogOpID: types.OpID{Term: 3, Index: 100},
	})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("expected pre-vote rejected: follower has active leader lease")
	}
}

func TestHandlePreVoteRejectedStaleCandidateLog(t *testing.T) {
	c, l := newConsensusForTest(t, "n-pv-3")
	defer l.Close()
	defer c.Stop()

	// Put some entries into the follower's log so it's ahead of the candidate.
	_, err := c.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term:     2,
		LeaderID: "old-leader",
		Entries: []wal.Entry{
			{OpID: types.OpID{Term: 2, Index: 1}, Payload: []byte("a")},
			{OpID: types.OpID{Term: 2, Index: 2}, Payload: []byte("b")},
		},
		LeaderCommit: types.OpID{Term: 2, Index: 2},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}

	// Wait for the follower lease to expire so it would otherwise grant.
	// (Use StepDown to clear the leader lease.)
	c.StepDown(2, "")

	// Candidate has a stale log — must be rejected.
	resp, err := c.HandlePreVote(context.Background(), raft.PreVoteRequest{
		NextTerm:    3,
		CandidateID: "cand-3",
		LastLogOpID: types.OpID{Term: 2, Index: 1}, // behind follower's index 2
	})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("expected pre-vote rejected: candidate log is stale")
	}
}

func TestHandlePreVoteLeaderRejectsWhileLeaseValid(t *testing.T) {
	c, l := newConsensusForTest(t, "n-pv-4")
	defer l.Close()
	defer c.Stop()

	c.BecomeLeader()

	resp, err := c.HandlePreVote(context.Background(), raft.PreVoteRequest{
		NextTerm:    2,
		CandidateID: "cand-4",
		LastLogOpID: types.OpID{Term: 1, Index: 99},
	})
	if err != nil {
		t.Fatalf("HandlePreVote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("leader with valid lease should reject pre-vote")
	}
}

func TestPreVoteDoesNotMutateTerm(t *testing.T) {
	c, l := newConsensusForTest(t, "n-pv-5")
	defer l.Close()
	defer c.Stop()

	before := c.State().CurrentTerm

	_, _ = c.HandlePreVote(context.Background(), raft.PreVoteRequest{
		NextTerm:    before + 5, // much higher term
		CandidateID: "cand-5",
		LastLogOpID: types.OpID{Term: before, Index: 1000},
	})

	if c.State().CurrentTerm != before {
		t.Fatalf("HandlePreVote must not mutate CurrentTerm: before=%d after=%d",
			before, c.State().CurrentTerm)
	}
}

// ── NeedsBootstrap in AppendEntriesResponse ───────────────────────────────────

func TestAppendEntriesResponseHasNeedsBootstrapField(t *testing.T) {
	c, l := newConsensusForTest(t, "n-nb-1")
	defer l.Close()
	defer c.Stop()

	resp, err := c.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term:     1,
		LeaderID: "leader-x",
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	// NeedsBootstrap is false for normal heartbeat.
	if resp.NeedsBootstrap {
		t.Fatalf("NeedsBootstrap should be false for normal heartbeat")
	}
}

func TestAppendEntriesResponseNeedsBootstrapWhenBehindBoundary(t *testing.T) {
	c, l := newConsensusForTest(t, "n-nb-2")
	defer l.Close()
	defer c.Stop()

	c.SetBootstrapBoundary(types.OpID{Term: 3, Index: 50})

	resp, err := c.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term:     3,
		LeaderID: "leader-x",
		Entries: []wal.Entry{{OpID: types.OpID{Term: 3, Index: 10}, Payload: []byte("x")}},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !resp.NeedsBootstrap {
		t.Fatalf("expected NeedsBootstrap=true when follower is behind boundary")
	}
}

func TestAppendEntriesResponseNoBootstrapWhenAtBoundary(t *testing.T) {
	c, l := newConsensusForTest(t, "n-nb-3")
	defer l.Close()
	defer c.Stop()

	c.SetBootstrapBoundary(types.OpID{Term: 4, Index: 20})

	resp, err := c.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term:     4,
		LeaderID: "leader-x",
		Entries: []wal.Entry{{OpID: types.OpID{Term: 4, Index: 20}, Payload: []byte("x")}},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if resp.NeedsBootstrap {
		t.Fatalf("expected NeedsBootstrap=false when follower reached boundary")
	}
}

// ── ChangeConfig ─────────────────────────────────────────────────────────────

func newLeaderForTest(t *testing.T, nodeID string) (*raft.Consensus, *wal.Log) {
	t.Helper()
	c, l := newConsensusForTest(t, nodeID)
	c.BecomeLeader()
	return c, l
}

func TestChangeConfigAddPeer(t *testing.T) {
	c, l := newLeaderForTest(t, "n-cc-1")
	defer l.Close()
	defer c.Stop()

	if err := c.ChangeConfig(context.Background(), raft.ConfigChangeOp{
		Type:   raft.AddPeer,
		PeerID: "n-cc-2",
	}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}

	peers := c.Peers()
	found := false
	for _, p := range peers {
		if p == "n-cc-2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("n-cc-2 not in peers after add: %v", peers)
	}
	if c.State().ConfigVersion < 1 {
		t.Fatalf("ConfigVersion should be at least 1 after change")
	}
}

func TestChangeConfigRemovePeer(t *testing.T) {
	c, l := newLeaderForTest(t, "n-cc-3")
	defer l.Close()
	defer c.Stop()

	_ = c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "n-cc-4"})
	_ = c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "n-cc-5"})

	if err := c.ChangeConfig(context.Background(), raft.ConfigChangeOp{
		Type:   raft.RemovePeer,
		PeerID: "n-cc-4",
	}); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	for _, p := range c.Peers() {
		if p == "n-cc-4" {
			t.Fatalf("n-cc-4 should be removed from peers")
		}
	}
}

func TestChangeConfigRejectsDuplicateAdd(t *testing.T) {
	c, l := newLeaderForTest(t, "n-cc-6")
	defer l.Close()
	defer c.Stop()

	_ = c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "dup-peer"})
	if err := c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "dup-peer"}); err == nil {
		t.Fatalf("expected error adding duplicate peer")
	}
}

func TestChangeConfigRejectsRemoveAbsent(t *testing.T) {
	c, l := newLeaderForTest(t, "n-cc-7")
	defer l.Close()
	defer c.Stop()

	if err := c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.RemovePeer, PeerID: "ghost"}); err == nil {
		t.Fatalf("expected error removing non-existent peer")
	}
}

func TestChangeConfigRejectsNonLeader(t *testing.T) {
	c, l := newConsensusForTest(t, "n-cc-8") // follower
	defer l.Close()
	defer c.Stop()

	if err := c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "x"}); err == nil {
		t.Fatalf("expected ErrNotLeader for follower ChangeConfig")
	}
}

func TestChangeConfigRejectsSelf(t *testing.T) {
	c, l := newLeaderForTest(t, "n-cc-9")
	defer l.Close()
	defer c.Stop()

	if err := c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "n-cc-9"}); err == nil {
		t.Fatalf("expected error adding self as peer")
	}
}

func TestChangeConfigPersistedAcrossReload(t *testing.T) {
	dir := t.TempDir()
	walLog, err := wal.NewLog(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	metaStore, err := raft.NewFileMetadataStore(dir)
	if err != nil {
		t.Fatalf("new meta store: %v", err)
	}
	c, err := raft.NewConsensus(raft.Config{NodeID: "n-persist"}, walLog, metaStore)
	if err != nil {
		t.Fatalf("new consensus: %v", err)
	}
	c.BecomeLeader()

	_ = c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "peer-a"})
	_ = c.ChangeConfig(context.Background(), raft.ConfigChangeOp{Type: raft.AddPeer, PeerID: "peer-b"})
	c.Stop()
	walLog.Close()

	// Reload from the same directory.
	walLog2, err := wal.NewLog(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("reload wal: %v", err)
	}
	defer walLog2.Close()
	c2, err := raft.NewConsensus(raft.Config{NodeID: "n-persist"}, walLog2, metaStore)
	if err != nil {
		t.Fatalf("reload consensus: %v", err)
	}
	defer c2.Stop()

	peers := c2.Peers()
	sort.Strings(peers)
	if len(peers) != 2 || peers[0] != "peer-a" || peers[1] != "peer-b" {
		t.Fatalf("expected [peer-a peer-b] after reload, got %v", peers)
	}
}
