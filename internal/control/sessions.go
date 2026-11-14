package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// A fixed slot pool bounds durable generations as well as cached results. Reopen
// deliberately retires a slot's old generation; old tokens are never re-executed.
const SessionSlots = 64

var ErrExpired = errors.New("session generation or result retired")

type SessionOpen struct {
	Slot               int    `json:"slot"`
	ExpectedGeneration uint64 `json:"expected_generation"`
	RequestID          string `json:"request_id"`
}
type SessionHandle struct {
	Slot       int    `json:"slot"`
	Generation uint64 `json:"generation"`
}
type SessionUpdate struct {
	Slot             int      `json:"slot"`
	Generation       uint64   `json:"generation"`
	Sequence         uint64   `json:"sequence"`
	ExpectedRevision uint64   `json:"expected_revision"`
	Workers          []Worker `json:"workers"`
}
type sessionRecord struct {
	Slot          int    `json:"slot"`
	Generation    uint64 `json:"generation"`
	OpenExpected  uint64 `json:"open_expected"`
	OpenRequestID string `json:"open_request_id"`
	Sequence      uint64 `json:"sequence"`
	Latest        *entry `json:"latest,omitempty"`
}

func ValidateSessionOpen(u SessionOpen) error {
	if u.Slot < 0 || u.Slot >= SessionSlots || len(u.RequestID) < 1 || len(u.RequestID) > 128 || !utf8.ValidString(u.RequestID) || u.ExpectedGeneration == ^uint64(0) {
		return fmt.Errorf("invalid session slot, registration ID, or generation")
	}
	return nil
}
func sessionCommand(u SessionUpdate) Update {
	return Update{RequestID: "session-update", ExpectedRevision: u.ExpectedRevision, Workers: cloneWorkers(u.Workers)}
}
func ValidateSessionUpdate(u SessionUpdate) error {
	if u.Slot < 0 || u.Slot >= SessionSlots || u.Generation == 0 || u.Sequence == 0 {
		return fmt.Errorf("invalid session slot, generation, or sequence")
	}
	return ValidateUpdate(sessionCommand(u))
}
func (m *Machine) sessionBudget(next *Machine) error {
	if _, err := next.Checkpoint(CheckpointPosition{Index: ^uint64(0), Term: ^uint64(0)}); err != nil {
		return fmt.Errorf("%w: %v", ErrCapacity, err)
	}
	return nil
}
func (m *Machine) OpenSession(u SessionOpen) (SessionHandle, error) {
	if err := ValidateSessionOpen(u); err != nil {
		return SessionHandle{}, err
	}
	old := m.sessions[u.Slot]
	if old.Generation > 0 && old.OpenRequestID == u.RequestID {
		if old.OpenExpected != u.ExpectedGeneration {
			return SessionHandle{}, ErrConflict
		}
		return SessionHandle{Slot: u.Slot, Generation: old.Generation}, nil
	}
	if u.ExpectedGeneration < old.Generation {
		return SessionHandle{}, ErrExpired
	}
	if u.ExpectedGeneration != old.Generation {
		return SessionHandle{}, ErrConflict
	}
	next := *m
	next.sessionsEnabled = true
	next.sessions[u.Slot] = sessionRecord{Slot: u.Slot, Generation: old.Generation + 1, OpenExpected: old.Generation, OpenRequestID: u.RequestID}
	if err := m.sessionBudget(&next); err != nil {
		return SessionHandle{}, err
	}
	*m = next
	return SessionHandle{Slot: u.Slot, Generation: old.Generation + 1}, nil
}
func (m *Machine) ApplySession(u SessionUpdate) (Snapshot, error) {
	if err := ValidateSessionUpdate(u); err != nil {
		return Snapshot{}, err
	}
	s := m.sessions[u.Slot]
	if u.Generation < s.Generation {
		return Snapshot{}, ErrExpired
	}
	if s.Generation == 0 || u.Generation != s.Generation {
		return Snapshot{}, ErrConflict
	}
	if u.Sequence < s.Sequence {
		return Snapshot{}, ErrExpired
	}
	command := sessionCommand(u)
	if u.Sequence == s.Sequence {
		if s.Latest == nil {
			return Snapshot{}, ErrConflict
		}
		a, _ := json.Marshal(s.Latest.Update)
		b, _ := json.Marshal(command)
		if string(a) != string(b) {
			return Snapshot{}, ErrConflict
		}
		return result(*s.Latest), nil
	}
	if s.Sequence == ^uint64(0) || u.Sequence != s.Sequence+1 || u.ExpectedRevision != m.state.Revision {
		return Snapshot{}, ErrConflict
	}
	if m.state.Revision == ^uint64(0) {
		return Snapshot{}, ErrCapacity
	}
	e := entry{Update: command, Revision: m.state.Revision + 1}
	next := *m
	next.state = result(e)
	s.Sequence = u.Sequence
	s.Latest = &e
	next.sessions[u.Slot] = s
	if err := m.sessionBudget(&next); err != nil {
		return Snapshot{}, err
	}
	*m = next
	return result(e), nil
}
