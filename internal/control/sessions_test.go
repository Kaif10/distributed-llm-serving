package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestSessionHistoryBoundedBeyondLifetimeLegacyLimit(t *testing.T) {
	m := NewMachine()
	open := SessionOpen{Slot: 0, RequestID: "open-client"}
	h, err := m.OpenSession(open)
	if err != nil {
		t.Fatal(err)
	}
	var latest SessionUpdate
	var bytesAt1000 int
	for i := uint64(1); i <= 2500; i++ {
		latest = SessionUpdate{Slot: 0, Generation: h.Generation, Sequence: i, ExpectedRevision: i - 1, Workers: []Worker{}}
		r, err := m.ApplySession(latest)
		if err != nil || r.Revision != i {
			t.Fatal(i, r, err)
		}
		if i%128 == 0 || i == 1000 {
			data, err := m.Checkpoint(CheckpointPosition{Index: i + 10, Term: 3})
			if err != nil {
				t.Fatal(err)
			}
			if i == 1000 {
				bytesAt1000 = len(data)
			}
			restored, _, err := RestoreCheckpoint(data)
			if err != nil {
				t.Fatal(err)
			}
			m = restored
		}
	}
	data, err := m.Checkpoint(CheckpointPosition{Index: 3000, Term: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > bytesAt1000+100 || len(m.requests) != 0 {
		t.Fatal("session history grows with lifetime writes", len(data), bytesAt1000)
	}
	if r, err := m.ApplySession(latest); err != nil || r.Revision != 2500 {
		t.Fatal("latest retry lost", r, err)
	}
	older := latest
	older.Sequence--
	if _, err := m.ApplySession(older); !errors.Is(err, ErrExpired) {
		t.Fatal("old sequence re-executed", err)
	}
	changed := latest
	changed.Workers = []Worker{{ID: "different", Origin: "http://127.0.0.1:1"}}
	if _, err := m.ApplySession(changed); !errors.Is(err, ErrConflict) {
		t.Fatal("same sequence changed meaning", err)
	}
	if r, err := m.OpenSession(open); err != nil || r != h {
		t.Fatal("registration retry changed generation", r, err)
	}
	if m.Snapshot().Revision != 2500 {
		t.Fatal("retry changed current revision")
	}
}

func TestSessionCheckpointRejectsCorruptFences(t *testing.T) {
	m := NewMachine()
	m.OpenSession(SessionOpen{Slot: 0, RequestID: "register"})
	m.ApplySession(SessionUpdate{Slot: 0, Generation: 1, Sequence: 1, Workers: []Worker{}})
	frame, err := m.Checkpoint(CheckpointPosition{Index: 3, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*checkpointPayload){
		"missing-generation":  func(r *checkpointPayload) { r.Sessions[0].Generation = 0 },
		"wrong-open-fence":    func(r *checkpointPayload) { r.Sessions[0].OpenExpected = 2 },
		"missing-latest":      func(r *checkpointPayload) { r.Sessions[0].Latest = nil },
		"impossible-sequence": func(r *checkpointPayload) { r.Sessions[0].Sequence = 10 },
		"state-mismatch":      func(r *checkpointPayload) { r.State.Workers = []Worker{{ID: "bad", Origin: "http://127.0.0.1:1"}} },
		"duplicate-slot":      func(r *checkpointPayload) { r.Sessions = append(r.Sessions, r.Sessions[0]) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			var r checkpointPayload
			json.Unmarshal(frame[12:], &r)
			mutate(&r)
			payload, _ := json.Marshal(r)
			if restored, _, err := RestoreCheckpoint(checkpointFrame(payload)); err == nil || restored != nil {
				t.Fatal("invalid session image accepted")
			}
		})
	}
}

func TestSessionReuseFencesOldGenerationAndPreservesLegacyResults(t *testing.T) {
	m := NewMachine()
	legacy := Update{RequestID: "legacy", Workers: []Worker{{ID: "original", Origin: "http://127.0.0.1:1"}}}
	if _, err := m.Apply(legacy); err != nil {
		t.Fatal(err)
	}
	h, err := m.OpenSession(SessionOpen{Slot: 2, RequestID: "client-one"})
	if err != nil {
		t.Fatal(err)
	}
	u := SessionUpdate{Slot: 2, Generation: h.Generation, Sequence: 1, ExpectedRevision: 1, Workers: []Worker{}}
	if _, err := m.ApplySession(u); err != nil {
		t.Fatal(err)
	}
	before := m
	data, _ := m.Checkpoint(CheckpointPosition{Index: 5, Term: 1})
	before, _, err = RestoreCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.OpenSession(SessionOpen{Slot: 2, ExpectedGeneration: 1, RequestID: "client-two"}); err != nil {
		t.Fatal(err)
	}
	if err := m.ValidateSuccessor(before); err != nil {
		t.Fatal("explicit generation retirement rejected", err)
	}
	data, err = m.Checkpoint(CheckpointPosition{Index: 6, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	m, _, err = RestoreCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ApplySession(u); !errors.Is(err, ErrExpired) {
		t.Fatal("retired generation re-executed", err)
	}
	if _, err := m.OpenSession(SessionOpen{Slot: 2, RequestID: "client-one"}); !errors.Is(err, ErrExpired) {
		t.Fatal("old registration resurrected", err)
	}
	u.Generation = 2
	u.ExpectedRevision = 2
	if r, err := m.ApplySession(u); err != nil || r.Revision != 3 {
		t.Fatal(r, err)
	}
	legacyNew := Update{RequestID: "after-session", ExpectedRevision: 3, Workers: []Worker{}}
	if _, err := m.ApplyCheckpointable(legacyNew); err != nil {
		t.Fatal(err)
	}
	data, err = m.Checkpoint(CheckpointPosition{Index: 8, Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	m, _, err = RestoreCheckpoint(data)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := m.ApplyCheckpointable(legacy); err != nil || r.Revision != 1 || r.Workers[0].ID != "original" {
		t.Fatal("legacy result changed", r, err)
	}
	if r, err := m.ApplySession(u); err != nil || r.Revision != 3 || m.Snapshot().Revision != 4 {
		t.Fatal("latest result not original after another API update", r, err)
	}
	// Changing a checkpoint without changing its revision is never a legal successor.
	clone, _, _ := RestoreCheckpoint(data)
	clone.state.Workers = []Worker{{ID: "forged", Origin: "http://127.0.0.1:1"}}
	if err := clone.ValidateSuccessor(m); err == nil {
		t.Fatal("state changed at same revision")
	}
}

func TestSessionConflictAndCanonicalEncoding(t *testing.T) {
	m := NewMachine()
	m.OpenSession(SessionOpen{Slot: 1, RequestID: "register"})
	u := SessionUpdate{Slot: 1, Generation: 1, Sequence: 2, Workers: []Worker{}}
	if _, err := m.ApplySession(u); !errors.Is(err, ErrConflict) {
		t.Fatal("sequence gap admitted", err)
	}
	u.Sequence = 1
	if _, err := m.ApplySession(u); err != nil {
		t.Fatal(err)
	}
	frame, err := m.Checkpoint(CheckpointPosition{Index: 3, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		again, err := m.Checkpoint(CheckpointPosition{Index: 3, Term: 1})
		if err != nil || !bytes.Equal(frame, again) {
			t.Fatal("noncanonical session image", err)
		}
	}
	for slot := 0; slot < SessionSlots; slot++ {
		if slot != 1 {
			if _, err := m.OpenSession(SessionOpen{Slot: slot, RequestID: fmt.Sprint(slot)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := m.OpenSession(SessionOpen{Slot: 64, RequestID: "overflow"}); err == nil {
		t.Fatal("unbounded slot admitted")
	}
}
