package raft

import "testing"

func TestFollowerRetainsOnlyDeliveredCommittedPrefix(t *testing.T) {
	for _, byteFull := range []bool{false, true} {
		t.Run(map[bool]string{false: "entries", true: "bytes"}[byteFull], func(t *testing.T) {
			s, err := OpenRetainedStorage(t.TempDir(), RetainedIdentity{Group: "full-follower", Member: "b"})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.Save(HardState{Term: 1}); err != nil {
				t.Fatal(err)
			}
			count := 1024
			if byteFull {
				count = 64
			}
			log := make([]Entry, count)
			for i := range log {
				log[i] = Entry{Index: uint64(i + 1), Term: 1, Noop: !byteFull}
				if byteFull {
					log[i].Data = make([]byte, 64<<10)
				}
			}
			if err := s.SaveLog(log); err != nil {
				t.Fatal(err)
			}
			n := replica(t, "b", []string{"a", "b", "c"}, s, 1, 16)
			image := RetainedSnapshot{Index: uint64(count), Term: 1, Data: []byte("opaque-core-image")}
			if err := n.RetainLocal(image); err == nil {
				t.Fatal("unconfirmed prefix was retained")
			}
			m := Message{Type: AppendEntries, From: "a", To: "b", Term: 2, PrevIndex: uint64(count), PrevTerm: 1, LeaderCommit: uint64(count), Probe: 1, Entries: []Entry{{Index: uint64(count + 1), Term: 2, Data: []byte("new")}}}
			replies, err := n.Step(m)
			if err != nil || len(replies) != 1 || !replies[0].CapacityRejected || replies[0].Success || n.CommitIndex() != uint64(count) {
				t.Fatal("full follower failed to acknowledge pressure/confirm matched prefix", replies, err)
			}
			if err := n.RetainLocal(image); err == nil {
				t.Fatal("undelivered committed prefix was retained")
			}
			if _, err := n.DrainCommitted(); err != nil {
				t.Fatal(err)
			}
			if err := n.RetainLocal(image); err != nil {
				t.Fatal("follower cannot retain its delivered committed prefix", err)
			}
			m.Probe++
			m.LeaderCommit++
			replies, err = n.Step(m)
			if err != nil || !replies[0].Success || n.CommitIndex() != uint64(count+1) {
				t.Fatal("follower did not recover after retention", replies, err)
			}
		})
	}
}

func TestAllHealthyPeersRetainAcrossMultipleBoundaries(t *testing.T) {
	ids := []string{"a", "b", "c"}
	nodes := map[string]*Node{}
	for i, id := range ids {
		s, err := OpenRetainedStorage(t.TempDir(), RetainedIdentity{Group: "healthy", Member: id})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		nodes[id] = replica(t, id, ids, s, int64(i+1), 16)
	}
	deliver(t, nodes, campaign(t, nodes["a"]))
	for i := 0; i < 384; i++ {
		out, err := nodes["a"].Propose([]byte("retry"))
		if err != nil {
			t.Fatal(err)
		}
		deliver(t, nodes, out)
		for _, id := range ids {
			n := nodes[id]
			if _, err := n.DrainCommitted(); err != nil {
				t.Fatal(err)
			}
			if n.CommitIndex()%32 == 0 {
				term, _ := n.TermAt(n.CommitIndex())
				if err := n.RetainLocal(RetainedSnapshot{Index: n.CommitIndex(), Term: term, Data: []byte("opaque-committed-core-image")}); err != nil {
					t.Fatal(id, err)
				}
			}
		}
	}
	for id, n := range nodes {
		if n.CommitIndex() != 385 || n.BaseIndex() < 352 || len(n.Log()) > 33 {
			t.Fatal("peer did not compact and continue", id, n.CommitIndex(), n.BaseIndex(), len(n.Log()))
		}
	}
}
