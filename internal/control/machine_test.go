package control

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMachineOrderedOutcomesAndReplay(t *testing.T) {
	commands := []Update{update("drain", 0, true), update("drain", 0, true), update("stale", 0, false), update("resume", 1, false), update("drain", 0, true), update("drain", 0, false), {RequestID: "empty", ExpectedRevision: 2}}
	wantRevisions := []uint64{1, 1, 0, 2, 1, 0, 3}
	for replica := 0; replica < 3; replica++ {
		m := NewMachine()
		for i, command := range commands {
			state, err := m.Apply(command)
			if wantRevisions[i] == 0 {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("command %d: expected conflict, got %v", i, err)
				}
			} else if err != nil || state.Revision != wantRevisions[i] {
				t.Fatalf("command %d: result %+v error %v", i, state, err)
			}
		}
		if state := m.Snapshot(); state.Revision != 3 || len(state.Workers) != 0 {
			t.Fatal("wrong replay state", state)
		}
		original, err := m.Apply(update("drain", 0, true))
		if err != nil || original.Revision != 1 || !original.Workers[0].Draining {
			t.Fatal("replay lost original retry result")
		}
	}
}

func TestMachineOwnsInputOutputAndRetryHistory(t *testing.T) {
	m := NewMachine()
	u := update("first", 0, true)
	state, err := m.Apply(u)
	if err != nil {
		t.Fatal(err)
	}
	u.Workers[0].ID = "changed-input"
	state.Workers[0].Draining = false
	view := m.Snapshot()
	view.Workers[0].Origin = "http://wrong"
	for i := 0; i < 2; i++ {
		old, err := m.Apply(update("first", 0, true))
		if err != nil || old.Workers[0].ID != "worker-a" || !old.Workers[0].Draining {
			t.Fatal("caller mutated retry history")
		}
		old.Workers[0].ID = "changed-result"
	}
	current := m.Snapshot()
	if current.Workers[0].Origin != "http://127.0.0.1:18081" || !current.Workers[0].Draining {
		t.Fatal("caller mutated current state")
	}
}

func TestMachineRejectsStringsThatCannotRoundTripJSON(t *testing.T) {
	for _, field := range []string{"request", "worker", "origin"} {
		m := NewMachine()
		u := update("first", 0, true)
		switch field {
		case "request":
			u.RequestID = string([]byte{255})
		case "worker":
			u.Workers[0].ID = string([]byte{255})
		case "origin":
			u.Workers[0].Origin = "http://" + string([]byte{255})
		}
		if _, err := m.Apply(u); err == nil {
			t.Fatal("accepted non-round-trippable field", field)
		}
		if m.Snapshot().Revision != 0 {
			t.Fatal("invalid command changed state")
		}
	}
}

func TestSeededMachineStoreAndRestartAgree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	m := NewMachine()
	replica := NewMachine()
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 300; i++ {
		request := update("request-"+string(rune('A'+rng.Intn(40))), uint64(rng.Intn(20)), rng.Intn(2) == 0)
		if i%3 == 0 {
			request.ExpectedRevision = m.Snapshot().Revision
		}
		a, ea := m.Apply(request)
		b, eb := replica.Apply(request)
		c, ec := s.Apply(request)
		failure := func(e error) string {
			if e == nil {
				return ""
			}
			return e.Error()
		}
		if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(a, c) || failure(ea) != failure(eb) || failure(ea) != failure(ec) {
			t.Fatalf("result divergence at command %d", i)
		}
		if i%37 == 0 {
			s.Close()
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
		}
		stored, err := s.Read()
		if err != nil || !reflect.DeepEqual(m.Snapshot(), stored) {
			t.Fatalf("state divergence at command %d", i)
		}
	}
}

func TestLegacyJournalEnvelopeStillReplays(t *testing.T) {
	// Literal pre-refactor wire fields, independent of current command marshaling.
	data := []byte(`{"update":{"request_id":"legacy","expected_revision":0,"workers":[{"id":"worker-a","origin":"http://127.0.0.1:18081","draining":true}]},"revision":1}`)
	frame := make([]byte, 12+len(data))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(data))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], data)
	path := filepath.Join(t.TempDir(), "legacy.wal")
	if err := os.WriteFile(path, frame, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	state, err := s.Read()
	if err != nil || state.Revision != 1 || !state.Workers[0].Draining {
		t.Fatal("legacy state lost", state, err)
	}
	var recorded entry
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatal(err)
	}
	if retry, err := s.Apply(recorded.Update); err != nil || !reflect.DeepEqual(retry, state) {
		t.Fatal("legacy retry failed", err)
	}
}
