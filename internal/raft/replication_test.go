package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type logMemory struct {
	memory
	entries []Entry
	failLog bool
}

func (s *logMemory) LoadLog() ([]Entry, error) { return cloneEntries(s.entries), nil }
func (s *logMemory) SaveLog(entries []Entry) error {
	if s.failLog {
		return errors.New("injected log sync failure")
	}
	s.entries = cloneEntries(entries)
	return nil
}
func replica(t *testing.T, id string, ids []string, s LogStorage, seed int64, batch int) *Node {
	t.Helper()
	n, err := NewReplica(Config{ID: id, Members: ids, ElectionTicks: 5, HeartbeatTicks: 1, Seed: seed, AppendBatch: batch}, s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func deliver(t *testing.T, nodes map[string]*Node, queue []Message) {
	t.Helper()
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 10000 {
			t.Fatal("message cycle did not settle")
		}
		m := queue[0]
		queue = queue[1:]
		out, err := nodes[m.To].Step(m)
		if err != nil {
			t.Fatal(err)
		}
		queue = append(queue, out...)
	}
}
func TestReplicaCommitsAndOwnsCommandBuffers(t *testing.T) {
	ids := []string{"a", "b", "c"}
	nodes := map[string]*Node{}
	for i, id := range ids {
		s, err := OpenStorage(filepath.Join(t.TempDir(), id+".wal"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		nodes[id] = replica(t, id, ids, s, int64(i+1), 16)
	}
	deliver(t, nodes, campaign(t, nodes["a"]))
	command := []byte("configuration-1")
	out, err := nodes["a"].Propose(command)
	if err != nil {
		t.Fatal(err)
	}
	command[0] = 'X'
	deliver(t, nodes, out)
	for _, id := range ids {
		n := nodes[id]
		if n.commit != 2 {
			t.Fatalf("%s commit=%d", id, n.commit)
		}
		entries, err := n.DrainCommitted()
		if err != nil || len(entries) != 2 || !entries[0].Noop || string(entries[1].Data) != "configuration-1" {
			t.Fatal("wrong committed delivery", entries, err)
		}
		entries[1].Data[0] = 'Y'
		if string(n.Log()[1].Data) != "configuration-1" {
			t.Fatal("caller changed log")
		}
		if again, _ := n.DrainCommitted(); len(again) != 0 {
			t.Fatal("delivery cursor repeated entries without restart")
		}
	}
}
func TestPriorTermMajorityWaitsForCurrentTermEntry(t *testing.T) {
	ids := []string{"a", "b", "c"}
	a := replica(t, "a", ids, &logMemory{memory: memory{state: HardState{Term: 1}}, entries: []Entry{{Index: 1, Term: 1, Data: []byte("old")}}}, 1, 1)
	b := replica(t, "b", ids, &logMemory{}, 2, 1)
	requests := campaign(t, a)
	vote, err := b.Step(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Step(vote[0])
	if err != nil {
		t.Fatal(err)
	}
	request := out[0]
	rejection, _ := b.Step(request)
	retry, err := a.Step(rejection[0])
	if err != nil {
		t.Fatal(err)
	}
	if retry[0].Entries[0].Term != 1 {
		t.Fatal("expected old-term catchup")
	}
	ack, _ := b.Step(retry[0])
	current, err := a.Step(ack[0])
	if err != nil {
		t.Fatal(err)
	}
	if a.commit != 0 || len(current) != 1 || current[0].Entries[0].Term != 2 {
		t.Fatal("old-term majority committed prematurely")
	}
	ack, err = b.Step(current[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Step(ack[0]); err != nil {
		t.Fatal(err)
	}
	if a.commit != 2 {
		t.Fatal("current-term majority did not commit preceding prefix")
	}
}
func TestShortAppendCannotCommitExtraSuffixAndConflictRepairs(t *testing.T) {
	s := &logMemory{memory: memory{state: HardState{Term: 2}}, entries: []Entry{{Index: 1, Term: 1, Data: []byte("base")}, {Index: 2, Term: 2, Data: []byte("orphan")}, {Index: 3, Term: 2, Data: []byte("extra")}}}
	n := replica(t, "a", []string{"a", "b", "c"}, s, 1, 16)
	out, err := n.Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 3, PrevIndex: 1, PrevTerm: 1, LeaderCommit: 3, Probe: 1})
	if err != nil || !out[0].Success || n.commit != 1 || n.lastIndex != 3 {
		t.Fatal("empty append committed/truncated unmatched suffix", n.commit, n.lastIndex, err)
	}
	out, err = n.Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 3, PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{Index: 2, Term: 3, Data: []byte("winner")}}, LeaderCommit: 2, Probe: 2})
	if err != nil || !out[0].Success || n.commit != 2 || n.lastIndex != 2 || string(n.entries[1].Data) != "winner" {
		t.Fatal("conflict repair failed", err)
	}
	_, err = n.Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 4, Entries: []Entry{{Index: 1, Term: 4, Data: []byte("bad")}}, Probe: 3})
	if err == nil || string(n.entries[0].Data) != "base" {
		t.Fatal("committed prefix replaced")
	}
}
func TestProbeRetransmitAndStaleRejection(t *testing.T) {
	n := replica(t, "a", []string{"a", "b", "c"}, &logMemory{}, 1, 16)
	campaign(t, n)
	out, err := n.Step(Message{Type: VoteResponse, From: "b", To: "a", Term: n.hard.Term, Granted: true})
	if err != nil {
		t.Fatal(err)
	}
	first := out[0]
	for i := 0; i < 5; i++ {
		again, _ := n.Tick()
		if again[0].Probe != first.Probe {
			t.Fatal("retry invalidated an outstanding acknowledgement")
		}
	}
	_, err = n.Step(Message{Type: AppendResponse, From: "b", To: "a", Term: n.hard.Term, Probe: first.Probe, Success: true, MatchIndex: 1})
	if err != nil {
		t.Fatal(err)
	}
	before := n.next["b"]
	n.Step(Message{Type: AppendResponse, From: "b", To: "a", Term: n.hard.Term, Probe: first.Probe, Success: false})
	if n.next["b"] != before {
		t.Fatal("stale failure regressed replication progress")
	}
}
func TestLogPersistenceFailureSendsNoAcknowledgement(t *testing.T) {
	s := &logMemory{failLog: true}
	n := replica(t, "b", []string{"a", "b", "c"}, s, 2, 16)
	out, err := n.Step(Message{Type: AppendEntries, From: "a", To: "b", Term: 1, Entries: []Entry{{Index: 1, Term: 1, Noop: true}}, Probe: 1})
	if !errors.Is(err, ErrStorage) || len(out) > 0 || !n.failed {
		t.Fatal("acknowledged failed follower persistence", out, err)
	}
}
func TestDurableLogReopenAndVolatileCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.wal")
	s, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	n := replica(t, "a", []string{"a"}, s, 1, 16)
	campaign(t, n)
	if _, err := n.Propose([]byte("value")); err != nil {
		t.Fatal(err)
	}
	before := n.Log()
	s.Close()
	s, err = OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n = replica(t, "a", []string{"a"}, s, 2, 16)
	if !reflect.DeepEqual(before, n.Log()) || n.commit != 0 {
		t.Fatal("durable log/volatile commit contract violated")
	}
	campaign(t, n)
	if n.commit != 3 {
		t.Fatal("new-term noop did not re-establish commit")
	}
}

func TestLegacyVoteJournalUpgradeAndIncompleteLogTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.wal")
	data := []byte(`{"kind":"raft-election-v1","state":{"term":3,"voted_for":"a"}}`)
	frame := make([]byte, 12+len(data))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(data))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], data)
	if err := os.WriteFile(path, frame, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	n := replica(t, "a", []string{"a"}, s, 1, 16)
	campaign(t, n)
	s.Close()
	base, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err = OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	n = replica(t, "a", []string{"a"}, s, 2, 16)
	campaign(t, n)
	if _, err := n.Propose([]byte("new")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, full[:len(base)+13], 0600); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	log, err := s.LoadLog()
	if err != nil || len(log) != 1 || !log[0].Noop || log[0].Term != 4 {
		t.Fatal("upgraded log lost on incomplete append", log, err)
	}
	if _, err := New(Config{ID: "a", Members: []string{"a"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 3}, s); err == nil {
		t.Fatal("election-only constructor ignored durable log")
	}
}

func TestSeededReplicationSafetyAcrossFaultSchedules(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ids := []string{"a", "b", "c", "d", "e"}
			rng := rand.New(rand.NewSource(seed))
			nodes := map[string]*Node{}
			stores := map[string]*logMemory{}
			for i, id := range ids {
				stores[id] = &logMemory{}
				nodes[id] = replica(t, id, ids, stores[id], seed*10+int64(i)+1, 2)
			}
			committed := map[uint64]Entry{}
			leaders := map[uint64]string{}
			var queue []Message
			check := func() {
				for _, id := range ids {
					n := nodes[id]
					if n.role == Leader {
						if other := leaders[n.hard.Term]; other != "" && other != id {
							t.Fatal("two leaders per term")
						}
						leaders[n.hard.Term] = id
					}
					for _, e := range n.entries[:n.commit] {
						if old, ok := committed[e.Index]; ok && (old.Term != e.Term || old.Noop != e.Noop || !bytes.Equal(old.Data, e.Data)) {
							t.Fatalf("conflicting committed index %d", e.Index)
						}
						committed[e.Index] = e
					}
				}
			}
			for tick := 0; tick < 250; tick++ {
				if tick < 140 && rng.Intn(20) == 0 {
					id := ids[rng.Intn(5)]
					nodes[id] = replica(t, id, ids, stores[id], seed*1000+int64(tick)+1, 2)
				}
				for _, id := range ids {
					out, err := nodes[id].Tick()
					if err != nil {
						t.Fatal(err)
					}
					queue = append(queue, out...)
					if nodes[id].role == Leader && tick%7 == 0 {
						out, err = nodes[id].Propose([]byte(fmt.Sprint(seed, "/", tick, "/", id)))
						if err != nil {
							t.Fatal(err)
						}
						queue = append(queue, out...)
					}
					check()
				}
				for delivery := 0; delivery < 100 && len(queue) > 0; delivery++ {
					i := rng.Intn(len(queue))
					m := queue[i]
					queue[i] = queue[len(queue)-1]
					queue = queue[:len(queue)-1]
					if tick < 140 && ((m.From == "a") != (m.To == "a") || rng.Intn(5) == 0) {
						continue
					}
					if tick < 140 && rng.Intn(8) == 0 {
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
			if count != 1 || len(committed) == 0 {
				t.Fatal("healed model did not make progress")
			}
		})
	}
}
