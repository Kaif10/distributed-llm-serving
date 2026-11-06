package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

type memory struct {
	state HardState
	fail  bool
}

func (s *memory) Load() (HardState, error) { return s.state, nil }
func (s *memory) Save(h HardState) error {
	if s.fail {
		return errors.New("injected write failure")
	}
	s.state = h
	return nil
}
func node(t *testing.T, id string, members []string, store Storage, seed int64) *Node {
	t.Helper()
	n, err := New(Config{ID: id, Members: members, ElectionTicks: 5, HeartbeatTicks: 1, Seed: seed}, store)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func campaign(t *testing.T, n *Node) []Message {
	t.Helper()
	for i := 0; i < 10; i++ {
		out, err := n.Tick()
		if err != nil {
			t.Fatal(err)
		}
		if len(out) > 0 || n.role == Leader {
			return out
		}
	}
	t.Fatal("no election")
	return nil
}
func TestDurableVoteSurvivesReopenAndRejectsSecondCandidate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.wal")
	s, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	n := node(t, "a", []string{"a", "b", "c"}, s, 1)
	out, err := n.Step(Message{Type: RequestVote, From: "b", To: "a", Term: 4})
	if err != nil || len(out) != 1 || !out[0].Granted {
		t.Fatal("vote not granted", out, err)
	}
	h, _ := s.Load()
	if h.Term != 4 || h.VotedFor != "b" {
		t.Fatal("response preceded durable vote")
	}
	s.Close()
	s, err = OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n = node(t, "a", []string{"a", "b", "c"}, s, 2)
	out, err = n.Step(Message{Type: RequestVote, From: "c", To: "a", Term: 4})
	if err != nil || out[0].Granted {
		t.Fatal("double vote after restart")
	}
	out, err = n.Step(Message{Type: RequestVote, From: "c", To: "a", Term: 5})
	if err != nil || !out[0].Granted {
		t.Fatal("new term vote denied")
	}
}
func TestMajorityAndDuplicateStaleVotes(t *testing.T) {
	n := node(t, "a", []string{"a", "b", "c", "d", "e"}, &memory{}, 1)
	campaign(t, n)
	term := n.hard.Term
	for i := 0; i < 3; i++ {
		n.Step(Message{Type: VoteResponse, From: "b", To: "a", Term: term, Granted: true})
	}
	n.Step(Message{Type: VoteResponse, From: "c", To: "a", Term: term + 1, Granted: false})
	if n.role != Follower || n.hard.Term != term+1 {
		t.Fatal("higher term did not step down")
	}
	campaign(t, n)
	term = n.hard.Term
	n.Step(Message{Type: VoteResponse, From: "b", To: "a", Term: term, Granted: true})
	n.Step(Message{Type: VoteResponse, From: "c", To: "a", Term: term - 1, Granted: true})
	if n.role == Leader {
		t.Fatal("duplicates or old votes formed majority")
	}
	n.Step(Message{Type: VoteResponse, From: "c", To: "a", Term: term, Granted: true})
	if n.role != Leader {
		t.Fatal("majority did not elect leader")
	}
}
func TestLogFreshnessAndRejectedVotesDoNotResetTimeout(t *testing.T) {
	n, err := New(Config{ID: "a", Members: []string{"a", "b", "c"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1, LastIndex: 8, LastTerm: 2}, &memory{state: HardState{Term: 3}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := n.deadline
	for i := 0; i < deadline-1; i++ {
		out, err := n.Step(Message{Type: RequestVote, From: "b", To: "a", Term: uint64(4 + i), LastIndex: 100, LastTerm: 1})
		if err != nil || out[0].Granted {
			t.Fatal("older log term granted")
		}
		n.Tick()
	}
	n.Tick()
	if n.role != Candidate {
		t.Fatal("rejected requests prevented election")
	}
}
func TestStorageFailuresFenceBeforeSendingMessages(t *testing.T) {
	for _, vote := range []bool{false, true} {
		s := &memory{fail: true}
		n := node(t, "a", []string{"a", "b", "c"}, s, 1)
		var out []Message
		var err error
		if vote {
			out, err = n.Step(Message{Type: RequestVote, From: "b", To: "a", Term: 1})
		} else {
			for i := 0; i < 10 && err == nil; i++ {
				out, err = n.Tick()
			}
		}
		if !errors.Is(err, ErrStorage) || len(out) != 0 || !n.failed {
			t.Fatal("unsafe output after failed persistence")
		}
		if out, err = n.Step(Message{Type: Heartbeat, From: "b", To: "a", Term: 9}); !errors.Is(err, ErrStorage) || len(out) != 0 {
			t.Fatal("fenced node continued")
		}
	}
}
func TestSplitVoteRetriesAndSingleNode(t *testing.T) {
	n := node(t, "a", []string{"a", "b", "c"}, &memory{}, 1)
	campaign(t, n)
	term := n.hard.Term
	n.Step(Message{Type: VoteResponse, From: "b", To: "a", Term: term, Granted: false})
	campaign(t, n)
	if n.hard.Term <= term || n.role != Candidate {
		t.Fatal("split vote did not retry")
	}
	single := node(t, "only", []string{"only"}, &memory{}, 2)
	campaign(t, single)
	if single.role != Leader {
		t.Fatal("self-majority failed")
	}
}

func TestHeartbeatTermsAndMembership(t *testing.T) {
	n := node(t, "a", []string{"a", "b", "c"}, &memory{state: HardState{Term: 3}}, 1)
	for i := 0; i < 25; i++ {
		n.Step(Message{Type: Heartbeat, From: "b", To: "a", Term: 3})
		n.Tick()
	}
	if n.role != Follower || n.hard.Term != 3 {
		t.Fatal("valid heartbeat did not maintain follower")
	}
	deadline := n.deadline
	for i := 0; i < deadline; i++ {
		n.Step(Message{Type: Heartbeat, From: "b", To: "a", Term: 2})
		n.Step(Message{Type: Heartbeat, From: "outsider", To: "a", Term: 99})
		n.Tick()
	}
	if n.role != Candidate || n.hard.Term != 4 {
		t.Fatal("stale or outsider heartbeats suppressed election", n.Status())
	}
	n.Step(Message{Type: Heartbeat, From: "b", To: "a", Term: 4})
	if n.role != Follower || n.hard.VotedFor != "a" {
		t.Fatal("same-term heartbeat lost durable vote")
	}
}

func TestLogTailTermPrecedesIndexInVoting(t *testing.T) {
	for _, tc := range []struct {
		index, term uint64
		grant       bool
	}{{100, 1, false}, {7, 2, false}, {8, 2, true}, {1, 3, true}} {
		n, err := New(Config{ID: "a", Members: []string{"a", "b", "c"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1, LastIndex: 8, LastTerm: 2}, &memory{state: HardState{Term: 3}})
		if err != nil {
			t.Fatal(err)
		}
		out, err := n.Step(Message{Type: RequestVote, From: "b", To: "a", Term: 4, LastIndex: tc.index, LastTerm: tc.term})
		if err != nil || out[0].Granted != tc.grant {
			t.Fatal("wrong log freshness decision", tc, out, err)
		}
	}
}

// This is a seeded transport model, not real network or multi-process evidence.
func TestSeededPartitionsReorderingDuplicationAndRestarts(t *testing.T) {
	for seed := int64(1); seed <= 50; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			ids := []string{"a", "b", "c", "d", "e"}
			nodes := map[string]*Node{}
			stores := map[string]*memory{}
			for i, id := range ids {
				stores[id] = &memory{}
				nodes[id] = node(t, id, ids, stores[id], seed*10+int64(i)+1)
			}
			var queue []Message
			leaders := map[uint64]string{}
			check := func() {
				for _, id := range ids {
					n := nodes[id]
					if n.role == Leader {
						if prior := leaders[n.hard.Term]; prior != "" && prior != id {
							t.Fatalf("two leaders term %d: %s/%s", n.hard.Term, prior, id)
						}
						leaders[n.hard.Term] = id
					}
				}
			}
			for tick := 0; tick < 180; tick++ {
				if tick < 100 && rng.Intn(15) == 0 {
					id := ids[rng.Intn(len(ids))]
					nodes[id] = node(t, id, ids, stores[id], seed*1000+int64(tick)+1)
				}
				for _, id := range ids {
					out, err := nodes[id].Tick()
					if err != nil {
						t.Fatal(err)
					}
					queue = append(queue, out...)
					check()
				}
				for delivery := 0; delivery < 60 && len(queue) > 0; delivery++ {
					i := rng.Intn(len(queue))
					m := queue[i]
					queue[i] = queue[len(queue)-1]
					queue = queue[:len(queue)-1]
					if tick < 100 && ((m.From == "a") != (m.To == "a") || rng.Intn(5) == 0) {
						continue
					}
					if tick < 100 && rng.Intn(8) == 0 {
						queue = append(queue, m)
					}
					out, err := nodes[m.To].Step(m)
					if err != nil {
						t.Fatal(err)
					}
					queue = append(queue, out...)
					check()
				}
			}
			count := 0
			for _, id := range ids {
				if nodes[id].role == Leader {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("healed model did not converge, leaders=%d", count)
			}
		})
	}
}
