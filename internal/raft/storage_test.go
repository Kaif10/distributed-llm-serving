package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestElectionStorageOwnershipTailAndCorruption(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "vote.wal")
	s, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := OpenStorage(path); err == nil {
		other.Close()
		t.Fatal("second owner accepted")
	}
	if err := s.Save(HardState{Term: 1, VotedFor: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(HardState{Term: 1, VotedFor: "c"}); err == nil {
		t.Fatal("conflicting vote persisted")
	}
	s.Close()
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err = OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(HardState{Term: 2, VotedFor: "c"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{len(first) + 1, len(first) + 11, len(first) + 12, len(full) - 1} {
		target := filepath.Join(root, "tail.wal")
		if err := os.WriteFile(target, full[:cut], 0600); err != nil {
			t.Fatal(err)
		}
		recovered, err := OpenStorage(target)
		if err != nil {
			t.Fatal(err)
		}
		h, err := recovered.Load()
		recovered.Close()
		if err != nil || h != (HardState{Term: 1, VotedFor: "b"}) {
			t.Fatal("previous durable vote lost", h, err)
		}
	}
	for _, index := range []int{0, len(full) - 1} {
		bad := append([]byte{}, full...)
		bad[index] ^= 1
		target := filepath.Join(root, "bad.wal")
		if err := os.WriteFile(target, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if recovered, err := OpenStorage(target); err == nil {
			recovered.Close()
			t.Fatal("corrupt state accepted")
		}
	}
}

func TestElectionUncertainSyncFencesAndRecoversVote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vote.wal")
	s, err := OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	s.syncFile = func() error { return errors.New("injected sync failure") }
	if err := s.Save(HardState{Term: 1, VotedFor: "b"}); err == nil {
		t.Fatal("sync failure hidden")
	}
	if _, err := s.Load(); !errors.Is(err, ErrStorage) {
		t.Fatal("uncertain state remained readable")
	}
	s.Close()
	s, err = OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h, err := s.Load()
	if err != nil || h != (HardState{Term: 1, VotedFor: "b"}) {
		t.Fatal("complete uncertain vote not recovered", h, err)
	}
	if err := s.Save(HardState{Term: 1, VotedFor: "c"}); err == nil {
		t.Fatal("uncertain vote allowed double voting")
	}
}
