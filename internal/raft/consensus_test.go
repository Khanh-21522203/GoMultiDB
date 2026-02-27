package raft_test

import (
	"context"

	"testing"

	"time"

	"GoMultiDB/internal/common/types"

	"GoMultiDB/internal/raft"

	"GoMultiDB/internal/wal"
)

func newConsensusForTest(t *testing.T, nodeID string) (*raft.Consensus, *wal.Log) {

	t.Helper()

	dir := t.TempDir()

	l, err := wal.NewLog(wal.Config{Dir: dir})

	if err != nil {

		t.Fatalf("new wal: %v", err)

	}

	meta, err := raft.NewFileMetadataStore(dir)

	if err != nil {

		t.Fatalf("new metadata store: %v", err)

	}

	c, err := raft.NewConsensus(raft.Config{

		NodeID: nodeID,

		ElectionTimeoutMin: 150 * time.Millisecond,

		ElectionTimeoutMax: 300 * time.Millisecond,
	}, l, meta)

	if err != nil {

		t.Fatalf("new consensus: %v", err)

	}

	return c, l

}

func TestConsensusLeaderReplicate(t *testing.T) {

	c, l := newConsensusForTest(t, "n1")

	defer l.Close()

	defer c.Stop()

	if err := c.Replicate(context.Background(), []wal.Entry{{OpID: types.OpID{Term: 1, Index: 1}, Payload: []byte("x")}}); err == nil {

		t.Fatalf("expected not leader error")

	}

	c.BecomeLeader()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	defer cancel()

	err := c.Replicate(ctx, []wal.Entry{{OpID: types.OpID{Term: 1, Index: 1}, Payload: []byte("x")}})

	if err != nil {

		t.Fatalf("replicate: %v", err)

	}

	st := c.State()

	if !st.CommittedOpID.Equal(types.OpID{Term: 1, Index: 1}) {

		t.Fatalf("committed op mismatch: %+v", st.CommittedOpID)

	}

}

func TestHandleAppendEntriesTermRules(t *testing.T) {

	c, l := newConsensusForTest(t, "n2")

	defer l.Close()

	defer c.Stop()

	c.StepDown(5, "leader-a")

	resp, err := c.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{

		Term: 4,

		LeaderID: "leader-b",
	})

	if err != nil {

		t.Fatalf("append entries: %v", err)

	}

	if resp.Success {

		t.Fatalf("expected reject for stale term")

	}

	resp, err = c.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{

		Term: 6,

		LeaderID: "leader-c",

		Entries: []wal.Entry{

			{OpID: types.OpID{Term: 6, Index: 1}, Payload: []byte("z")},
		},

		LeaderCommit: types.OpID{Term: 6, Index: 1},
	})

	if err != nil {

		t.Fatalf("expected success for newer term, got: %v", err)

	}

	if !resp.Success {

		t.Fatalf("expected success for newer term")

	}

	st := c.State()

	if st.CurrentTerm != 6 {

		t.Fatalf("expected term 6 got %d", st.CurrentTerm)

	}

	if st.Role != raft.Follower {

		t.Fatalf("expected follower, got %v", st.Role)

	}

}

func TestRequestVotePersistsVote(t *testing.T) {

	dir := t.TempDir()

	l, err := wal.NewLog(wal.Config{Dir: dir})

	if err != nil {

		t.Fatalf("new wal: %v", err)

	}

	defer l.Close()

	meta, err := raft.NewFileMetadataStore(dir)

	if err != nil {

		t.Fatalf("new metadata store: %v", err)

	}

	c, err := raft.NewConsensus(raft.Config{NodeID: "n3"}, l, meta)

	if err != nil {

		t.Fatalf("new consensus: %v", err)

	}

	defer c.Stop()

	_, err = c.HandleRequestVote(context.Background(), raft.VoteRequest{

		Term: 7,

		CandidateID: "cand-1",

		LastLogOpID: types.OpID{},
	})

	if err != nil {

		t.Fatalf("handle vote: %v", err)

	}

	loaded, err := meta.Load()

	if err != nil {

		t.Fatalf("load metadata: %v", err)

	}

	if loaded.CurrentTerm != 7 {

		t.Fatalf("expected term 7 got %d", loaded.CurrentTerm)

	}

	if loaded.VotedFor != "cand-1" {

		t.Fatalf("expected voted_for cand-1 got %s", loaded.VotedFor)

	}

}

func TestElectionTimerPromotesCandidate_DeterministicTick(t *testing.T) {

	dir := t.TempDir()

	l, err := wal.NewLog(wal.Config{Dir: dir})

	if err != nil {

		t.Fatalf("new wal: %v", err)

	}

	defer l.Close()

	meta, err := raft.NewFileMetadataStore(dir)

	if err != nil {

		t.Fatalf("new metadata store: %v", err)

	}

	tickCh := make(chan struct{}, 1)

	c, err := raft.NewConsensus(raft.Config{

		NodeID: "n4",

		ElectionTickCh: tickCh,
	}, l, meta)

	if err != nil {

		t.Fatalf("new consensus: %v", err)

	}

	defer c.Stop()

	st0 := c.State()

	c.Start()

	tickCh <- struct{}{}

	deadline := time.Now().Add(500 * time.Millisecond)

	for time.Now().Before(deadline) {

		st := c.State()

		if st.CurrentTerm > st0.CurrentTerm && st.Role == raft.Candidate {

			return

		}

		time.Sleep(5 * time.Millisecond)

	}

	st := c.State()

	t.Fatalf("expected candidate with incremented term, got role=%v term=%d", st.Role, st.CurrentTerm)

}
