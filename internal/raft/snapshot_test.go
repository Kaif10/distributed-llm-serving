package raft

import (
	"errors"
	"testing"
)

func pumpSnapshot(t *testing.T, nodes map[string]*Node, queue []Message, offline string) {
	t.Helper()
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 10000 {
			t.Fatal("snapshot message cycle")
		}
		m := queue[0]
		queue = queue[1:]
		if m.To == offline {
			continue
		}
		out, err := nodes[m.To].Step(m)
		if err != nil {
			t.Fatal(err)
		}
		queue = append(queue, out...)
	}
}

func TestSnapshotTransferRetrySuffixAndReopen(t *testing.T) {
	ids := []string{"a", "b", "c"}
	nodes := map[string]*Node{}
	stores := map[string]*RetainedStorage{}
	dirs := map[string]string{}
	for _, id := range ids {
		dirs[id] = t.TempDir()
		s, err := OpenRetainedStorage(dirs[id], RetainedIdentity{Group: "transfer", Member: id})
		if err != nil {
			t.Fatal(err)
		}
		stores[id] = s
		defer func() { stores[id].Close() }()
		nodes[id] = replica(t, id, ids, s, int64(len(nodes)+1), 16)
	}
	a, c := nodes["a"], nodes["c"]
	pumpSnapshot(t, nodes, campaign(t, a), "")
	out, err := a.Propose([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	pumpSnapshot(t, nodes, out, "")
	for _, text := range []string{"second", "third"} {
		out, err = a.Propose([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		pumpSnapshot(t, nodes, out, "c")
	}
	if _, err := a.DrainCommitted(); err != nil {
		t.Fatal(err)
	}
	if err := a.RetainLocal(RetainedSnapshot{Index: 4, Term: a.hard.Term, Data: []byte("opaque-committed-image")}); err != nil {
		t.Fatal(err)
	}
	out, err = a.Propose([]byte("suffix"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot Message
	for _, m := range out {
		if m.To == "c" {
			snapshot = m
		}
	}
	if snapshot.Type != InstallSnapshot || snapshot.Snapshot.Index != 4 {
		t.Fatal("missing snapshot", snapshot)
	}
	// Correlated but false acknowledgements cannot count toward replication.
	if out, err := a.Step(Message{Type: SnapshotResponse, From: "c", To: "a", Term: a.hard.Term, Probe: snapshot.Probe, MatchIndex: 5, Success: true}); err != nil || len(out) != 0 || a.match["c"] >= 4 {
		t.Fatal("accepted wrong snapshot acknowledgement", err)
	}
	for _, m := range out {
		if m.To != "c" {
			pumpSnapshot(t, nodes, []Message{m}, "c")
		}
	}
	ack, err := c.Step(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if c.BaseIndex() != 4 || c.commit != 4 {
		t.Fatal("image not installed before acknowledgement")
	}
	generation := stores["c"].image.Generation
	// Lose the first reply; replaying the same complete image must not republish.
	again, err := c.Step(snapshot)
	if err != nil || len(again) != 1 || stores["c"].image.Generation != generation {
		t.Fatal("snapshot retry republished", err)
	}
	if len(ack) != 1 {
		t.Fatal(ack)
	}
	copyMessage := a.sendAppend("c")
	copyMessage.Snapshot.Data[0] = 'X'
	if string(a.pending["c"].Snapshot.Data) != "opaque-committed-image" {
		t.Fatal("sender snapshot aliases transport")
	}
	pumpSnapshot(t, nodes, again, "")
	if c.commit != 5 || string(c.Log()[0].Data) != "suffix" {
		t.Fatal("suffix did not catch up", c.commit, c.Log())
	}
	if err := stores["c"].Close(); err != nil {
		t.Fatal(err)
	}
	stores["c"], err = OpenRetainedStorage(dirs["c"], RetainedIdentity{Group: "transfer", Member: "c"})
	if err != nil {
		t.Fatal(err)
	}
	c = replica(t, "c", ids, stores["c"], 9, 16)
	nodes["c"] = c
	if c.commit != 4 || len(c.Log()) != 1 {
		t.Fatal("reopen lost coherent snapshot/suffix")
	}
	pumpSnapshot(t, nodes, a.heartbeats(), "")
	if c.commit != 5 {
		t.Fatal("reopened suffix not confirmed")
	}
	before := stores["c"].image.Generation
	if _, err := c.Step(snapshot); err != nil || stores["c"].image.Generation != before || c.commit != 5 {
		t.Fatal("stale image rolled back commit", err)
	}
	wrong := snapshot
	wrong.Group = "other-group"
	if _, err := c.Step(wrong); err == nil {
		t.Fatal("cross-group image accepted")
	}
}

func TestSnapshotInstallUncertainSyncFencesAndReopens(t *testing.T) {
	dir := t.TempDir()
	id := RetainedIdentity{Group: "transfer", Member: "c"}
	s, err := OpenRetainedStorage(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	n := replica(t, "c", []string{"a", "b", "c"}, s, 1, 16)
	// Persist the incoming term before injecting the snapshot publication fault.
	if _, err := n.Step(Message{Type: ReadProbe, From: "a", To: "c", Term: 2, ReadID: 1}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.syncSlot = func(slot int) error {
		calls++
		if calls == 2 {
			return errors.New("injected install sync failure")
		}
		return s.slots[slot].Sync()
	}
	image := RetainedSnapshot{Index: 10, Term: 1, Data: []byte("opaque-committed-image")}
	out, err := n.Step(Message{Type: InstallSnapshot, From: "a", To: "c", Term: 2, Probe: 1, Group: id.Group, Snapshot: &image})
	if !errors.Is(err, ErrStorage) || len(out) != 0 || !n.failed {
		t.Fatal("uncertain installation acknowledged", out, err)
	}
	s.Close()
	s, err = OpenRetainedStorage(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	n = replica(t, "c", []string{"a", "b", "c"}, s, 2, 16)
	if n.BaseIndex() != 10 || n.commit != 10 || n.hard.Term != 2 {
		t.Fatal("complete uncertain image not selected after reopen")
	}
	if err := s.Save(HardState{Term: 3, VotedFor: "c"}); err != nil {
		t.Fatal("normal vote after installed generation failed", err)
	}
}
