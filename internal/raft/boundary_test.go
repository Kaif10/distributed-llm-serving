package raft

import (
	"errors"
	"reflect"
	"testing"
)

// A retained image fixture models the storage adapter's coherent snapshot/suffix
// contract. No file compaction or network snapshot transfer is implied by it.
type retainedMemory struct {
	logMemory
	snapshot RetainedSnapshot
}

func (s *retainedMemory) LoadSnapshot() (RetainedSnapshot, error) { return s.snapshot, nil }
func retained(t *testing.T, id string, ids []string, index, term uint64, entries []Entry) *Node {
	t.Helper()
	store := &retainedMemory{logMemory: logMemory{memory: memory{state: HardState{Term: term}}, entries: entries}, snapshot: RetainedSnapshot{Index: index, Term: term, Data: []byte("opaque-application-image")}}
	return replica(t, id, ids, store, 7, 16)
}

func TestRetainedBoundaryReopenElectionDeliveryAndRead(t *testing.T) {
	ids := []string{"a"}
	n := retained(t, "a", ids, 1500, 2, nil)
	if n.commit != 1500 || n.applied != 1500 || n.lastIndex != 1500 {
		t.Fatal("boundary not restored")
	}
	if entries, err := n.DrainCommitted(); err != nil || len(entries) != 0 {
		t.Fatal(entries, err)
	}
	if _, _, err := n.BeginRead(); !errors.Is(err, ErrNotLeader) {
		t.Fatal("follower read was ready", err)
	}
	campaign(t, n)
	if n.commit != 1501 || n.Log()[0].Index != 1501 || !n.Log()[0].Noop {
		t.Fatal("election renumbered retained suffix")
	}
	token, _, err := n.BeginRead()
	if err != nil || token.Index != 1501 {
		t.Fatal(token, err)
	}
	if ready, err := n.ReadReady(token); err != nil || !ready {
		t.Fatal(ready, err)
	}
	n.ForgetRead(token)
	if _, err := n.Propose([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	entries, err := n.DrainCommitted()
	if err != nil || len(entries) != 2 || entries[0].Index != 1501 || entries[1].Index != 1502 {
		t.Fatal(entries, err)
	}
	reopened := replica(t, "a", ids, n.logStorage, 9, 16)
	if reopened.commit != 1500 || reopened.applied != 1500 {
		t.Fatal("unconfirmed suffix exposed on reopen")
	}
	campaign(t, reopened)
	entries, err = reopened.DrainCommitted()
	if err != nil || len(entries) != 3 || entries[0].Index != 1501 || entries[2].Index != 1503 || string(entries[1].Data) != "tail" {
		t.Fatal("reopen lost absolute suffix", entries, err)
	}
}

func TestRetainedBoundaryLookupAndByteOwnership(t *testing.T) {
	n := retained(t, "a", []string{"a", "b", "c"}, 100, 2, []Entry{{Index: 101, Term: 2, Data: []byte("suffix")}})
	for index, want := range map[uint64]uint64{100: 2, 101: 2} {
		term, ok := n.TermAt(index)
		if !ok || term != want {
			t.Fatal(index, term, ok)
		}
	}
	for _, index := range []uint64{0, 99, 102} {
		if _, ok := n.TermAt(index); ok {
			t.Fatal("term fabricated outside retained range", index)
		}
	}
	image := n.Snapshot()
	image.Data[0] = 'X'
	source := n.logStorage.(*retainedMemory)
	source.snapshot.Data[0] = 'Y'
	if string(n.Snapshot().Data) != "opaque-application-image" {
		t.Fatal("snapshot bytes alias caller/storage")
	}
	if _, err := New(Config{ID: "a", Members: []string{"a", "b", "c"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1}, source); err == nil {
		t.Fatal("election-only constructor ignored snapshot")
	}
}

func TestRetainedFollowerPrefixHintsConflictAndCommittedProtection(t *testing.T) {
	n := retained(t, "a", []string{"a", "b", "c"}, 100, 2, []Entry{{Index: 101, Term: 2, Data: []byte("old")}, {Index: 102, Term: 2, Data: []byte("orphan")}})
	response, err := n.Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 3, PrevIndex: 90, PrevTerm: 2, Probe: 1})
	if err != nil || len(response) != 1 || response[0].Success || response[0].HintIndex != 100 || response[0].HintTerm != 2 || n.commit != 100 {
		t.Fatal(response, err)
	}
	response, err = n.Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 3, PrevIndex: 100, PrevTerm: 1, Probe: 2})
	if err != nil || response[0].Success {
		t.Fatal("accepted wrong boundary term", err)
	}
	response, err = n.Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 3, PrevIndex: 100, PrevTerm: 2, Entries: []Entry{{Index: 101, Term: 3, Data: []byte("winner")}}, LeaderCommit: 101, Probe: 3})
	if err != nil || !response[0].Success || n.commit != 101 || len(n.Log()) != 1 || string(n.Log()[0].Data) != "winner" {
		t.Fatal("offset conflict repair failed", response, err)
	}
	before := n.Log()
	_, err = n.Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 4, PrevIndex: 100, PrevTerm: 2, Entries: []Entry{{Index: 101, Term: 4, Data: []byte("bad")}}, Probe: 4})
	if err == nil || !reflect.DeepEqual(before, n.Log()) {
		t.Fatal("replaced committed retained suffix", err)
	}
}

func TestRetainedHintDoesNotAcknowledgeReplication(t *testing.T) {
	ids := []string{"a", "b", "c"}
	entries := make([]Entry, 200)
	for i := range entries {
		entries[i] = Entry{Index: uint64(i) + 1, Term: 2, Noop: true}
	}
	a := replica(t, "a", ids, &logMemory{memory: memory{state: HardState{Term: 2}}, entries: entries}, 1, 16)
	b := retained(t, "b", ids, 100, 2, nil)
	c := replica(t, "c", ids, &logMemory{memory: memory{state: HardState{Term: 2}}, entries: entries}, 3, 16)
	vote, err := b.Step(campaign(t, a)[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Step(vote[0]); err != nil {
		t.Fatal(err)
	}
	// Model a conservative next-index estimate before the leader learns the
	// follower has already retained a later committed boundary.
	a.pending = make(map[string]Message)
	a.next["b"] = 1
	request := a.sendAppend("b")
	hint, err := b.Step(request)
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Step(hint[0])
	if err != nil || len(out) != 1 || out[0].PrevIndex != 100 || a.match["b"] != 0 || a.commit != 0 {
		t.Fatal("hint was counted as an acknowledgement", out, err)
	}
	deliver(t, map[string]*Node{"a": a, "b": b, "c": c}, out)
	if a.commit != 201 || b.commit != 201 {
		t.Fatal("replication did not continue above boundary", a.commit, b.commit)
	}
}

func TestRetainedLeaderMarksPeerNeedingSnapshotWithoutInvalidAppend(t *testing.T) {
	ids := []string{"a", "b", "c"}
	a := retained(t, "a", ids, 100, 2, nil)
	b := replica(t, "b", ids, &logMemory{}, 2, 16)
	vote, err := b.Step(campaign(t, a)[0])
	if err != nil {
		t.Fatal(err)
	}
	messages, err := a.Step(vote[0])
	if err != nil {
		t.Fatal(err)
	}
	var request Message
	for _, m := range messages {
		if m.To == "b" {
			request = m
		}
	}
	reply, err := b.Step(request)
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Step(reply[0])
	if err != nil || len(out) != 0 {
		t.Fatal("sent append into discarded prefix", out, err)
	}
	if !reflect.DeepEqual(a.PendingSnapshotPeers(), []string{"b"}) || a.commit != 100 {
		t.Fatal("missing snapshot requirement")
	}
	tick, err := a.Tick()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range tick {
		if m.To == "" || m.To == "b" {
			t.Fatal("emitted invalid/unsupported catch-up", m)
		}
	}
	if _, _, err := a.BeginRead(); !errors.Is(err, ErrReadNotReady) {
		t.Fatal("uncommitted new leader served read", err)
	}
}

func TestRetainedValidationAndIndexSentinel(t *testing.T) {
	for name, snapshot := range map[string]RetainedSnapshot{
		"zero-index": {Term: 1, Data: []byte("x")}, "missing-term": {Index: 1, Data: []byte("x")},
		"missing-image": {Index: 1, Term: 1}, "ahead-term": {Index: 1, Term: 3, Data: []byte("x")},
		"max-index": {Index: ^uint64(0), Term: 1, Data: []byte("x")},
	} {
		t.Run(name, func(t *testing.T) {
			s := &retainedMemory{logMemory: logMemory{memory: memory{state: HardState{Term: 2}}}, snapshot: snapshot}
			if _, err := NewReplica(Config{ID: "a", Members: []string{"a"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1}, s); err == nil {
				t.Fatal("invalid retained image accepted")
			}
		})
	}
	ids := []string{"a", "b", "c"}
	nodes := map[string]*Node{}
	for _, id := range ids {
		nodes[id] = retained(t, id, ids, ^uint64(0)-2, 1, nil)
	}
	deliver(t, nodes, campaign(t, nodes["a"]))
	if nodes["a"].commit != ^uint64(0)-1 {
		t.Fatal("high-index no-op did not commit")
	}
	if _, err := nodes["a"].Propose([]byte("overflow")); err == nil {
		t.Fatal("index wrapped")
	}
	for _, n := range nodes {
		out, err := n.DrainCommitted()
		if err != nil || len(out) != 1 || out[0].Index != ^uint64(0)-1 {
			t.Fatal(out, err)
		}
	}
	if _, err := nodes["a"].Step(Message{Type: AppendEntries, From: "b", To: "a", Term: 3, PrevIndex: ^uint64(0), PrevTerm: 2}); err == nil {
		t.Fatal("overflowing append prefix accepted")
	}
}
