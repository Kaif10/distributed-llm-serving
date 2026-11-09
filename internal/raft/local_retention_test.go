package raft

import (
	"errors"
	"testing"
)

func TestLocalRetentionDeliveryReadAndNextAbsoluteIndex(t *testing.T) {
	s, err := OpenRetainedStorage(t.TempDir(), retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n := replica(t, "a", []string{"a"}, s, 1, 16)
	campaign(t, n)
	if _, err := n.Propose([]byte("command")); err != nil {
		t.Fatal(err)
	}
	image := RetainedSnapshot{Index: 2, Term: n.hard.Term, Data: []byte("opaque-image")}
	if err := n.RetainLocal(image); err == nil {
		t.Fatal("retained before delivery to the application owner")
	}
	if _, err := n.DrainCommitted(); err != nil {
		t.Fatal(err)
	}
	wrong := image
	wrong.Term++
	if err := n.RetainLocal(wrong); err == nil {
		t.Fatal("wrong boundary term accepted")
	}
	token, _, err := n.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer n.ForgetRead(token)
	if err := n.RetainLocal(image); err != nil {
		t.Fatal(err)
	}
	image.Data[0] = 'X'
	if n.BaseIndex() != 2 || n.commit != 2 || n.applied != 2 || len(n.Log()) != 0 || string(n.Snapshot().Data) != "opaque-image" {
		t.Fatal("in-memory boundary differs from durable selection")
	}
	if ready, err := n.ReadReady(token); err != nil || !ready {
		t.Fatal("same-term read token invalidated", ready, err)
	}
	if _, err := n.Propose([]byte("next")); err != nil {
		t.Fatal(err)
	}
	entries, err := n.DrainCommitted()
	if err != nil || len(entries) != 1 || entries[0].Index != 3 || string(entries[0].Data) != "next" {
		t.Fatal(entries, err)
	}
	stored, _ := s.LoadSnapshot()
	if stored.Index != 2 || string(stored.Data) != "opaque-image" {
		t.Fatal("durable image bytes changed")
	}
}

func TestLocalRetentionUncertainStorageFencesCore(t *testing.T) {
	s, err := OpenRetainedStorage(t.TempDir(), retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n := replica(t, "a", []string{"a"}, s, 1, 16)
	campaign(t, n)
	if _, err := n.DrainCommitted(); err != nil {
		t.Fatal(err)
	}
	calls := 0
	s.syncSlot = func(slot int) error {
		calls++
		if calls == 2 {
			return errors.New("injected publication sync failure")
		}
		return s.slots[slot].Sync()
	}
	if err := n.RetainLocal(RetainedSnapshot{Index: 1, Term: n.hard.Term, Data: []byte("opaque")}); !errors.Is(err, ErrStorage) {
		t.Fatal("uncertain retention was not fenced", err)
	}
	if !n.failed || n.role != Follower || n.BaseIndex() != 0 {
		t.Fatal("core exposed uncertain retained selection")
	}
	if _, _, err := n.BeginRead(); !errors.Is(err, ErrStorage) {
		t.Fatal("uncertain core served reads", err)
	}
	if _, err := n.Propose([]byte("bad")); !errors.Is(err, ErrStorage) {
		t.Fatal("uncertain core accepted writes", err)
	}
}

func TestLocalRetentionRejectsFollowerAndWrongMemberStorage(t *testing.T) {
	s, err := OpenRetainedStorage(t.TempDir(), retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n := replica(t, "a", []string{"a", "b", "c"}, s, 1, 16)
	if !n.CanRetainLocal() {
		t.Fatal("retained storage must support snapshot-capable membership")
	}
	if err := n.RetainLocal(RetainedSnapshot{Index: 1, Term: 1, Data: []byte("opaque")}); err == nil {
		t.Fatal("follower retention accepted")
	}
	if _, err := NewReplica(Config{ID: "b", Members: []string{"b"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1}, s); err == nil {
		t.Fatal("member opened another member's retained state")
	}
}
