package control

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func checkpointMachine(t *testing.T) *Machine {
	t.Helper()
	m := NewMachine()
	for i, key := range []string{"drain", "resume"} {
		if _, err := m.Apply(update(key, uint64(i), i == 0)); err != nil {
			t.Fatal(err)
		}
	}
	return m
}
func TestCheckpointRestoresHistoricalRetriesAndOwnsBuffers(t *testing.T) {
	m := checkpointMachine(t)
	position := CheckpointPosition{Index: 5, Term: 3}
	data, err := m.Checkpoint(position)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		again, err := m.Checkpoint(position)
		if err != nil || !bytes.Equal(data, again) {
			t.Fatal("nondeterministic checkpoint", err)
		}
	}
	restored, got, err := RestoreCheckpoint(data)
	if err != nil || got != position || !reflect.DeepEqual(restored.Snapshot(), m.Snapshot()) {
		t.Fatal("bad restoration", got, err)
	}
	original, err := restored.Apply(update("drain", 0, true))
	if err != nil || original.Revision != 1 || !original.Workers[0].Draining {
		t.Fatal("lost historical retry", original, err)
	}
	original.Workers[0].ID = "mutated-output"
	data[len(data)-1] ^= 1
	retry, err := restored.Apply(update("drain", 0, true))
	if err != nil || retry.Workers[0].ID != "worker-a" {
		t.Fatal("checkpoint/output alias", err)
	}
	if _, err := restored.Apply(update("drain", 2, false)); !errors.Is(err, ErrConflict) {
		t.Fatal("reused ID did not conflict", err)
	}
	if restored.Snapshot().Revision != 2 {
		t.Fatal("retry rewound current state")
	}
	next, err := restored.Apply(update("next", 2, true))
	if err != nil || next.Revision != 3 {
		t.Fatal(next, err)
	}
	if m.Snapshot().Revision != 2 {
		t.Fatal("restoration mutated original machine")
	}
}

func TestCheckpointFramingAndCanonicalEncoding(t *testing.T) {
	data, err := checkpointMachine(t).Checkpoint(CheckpointPosition{Index: 3, Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	reject := func(frame []byte) {
		t.Helper()
		machine, position, err := RestoreCheckpoint(frame)
		if err == nil || machine != nil || position != (CheckpointPosition{}) {
			t.Fatal("published invalid checkpoint", err)
		}
	}
	for cut := 0; cut < len(data); cut++ {
		reject(data[:cut])
	}
	for _, offset := range []int{0, 4, 8, 12, len(data) - 1} {
		damaged := bytes.Clone(data)
		damaged[offset] ^= 1
		reject(damaged)
	}
	reject(append(bytes.Clone(data), 0))
	reject(append(bytes.Clone(data), data...))
	reject(make([]byte, 12+maxCheckpointPayload+1))
	// Valid checksums must not hide ambiguous/noncanonical JSON representations.
	reject(checkpointFrame(append(bytes.Clone(data[12:]), ' ')))
	duplicate := bytes.Replace(data[12:], []byte(`"revision":2`), []byte(`"revision":1,"revision":2`), 1)
	reject(checkpointFrame(duplicate))
	extra := append(bytes.Clone(data[12:len(data)-1]), []byte(`,"unknown":1}`)...)
	reject(checkpointFrame(extra))
}

// Independent fixture framing lets semantic corruption retain valid checksums.
func checkpointFrame(payload []byte) []byte {
	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header, uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(header[8:], crc32.ChecksumIEEE(header[:8]))
	return append(header, payload...)
}
func TestCheckpointRejectsInvalidSuccessfulHistory(t *testing.T) {
	data, err := checkpointMachine(t).Checkpoint(CheckpointPosition{Index: 5, Term: 3})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*checkpointPayload){
		"version":               func(p *checkpointPayload) { p.Kind = "fleet-application-v2" },
		"missing-history":       func(p *checkpointPayload) { p.Requests = p.Requests[:1] },
		"wrong-state":           func(p *checkpointPayload) { p.State.Workers[0].Draining = true },
		"wrong-count":           func(p *checkpointPayload) { p.State.Revision++ },
		"order":                 func(p *checkpointPayload) { p.Requests[0], p.Requests[1] = p.Requests[1], p.Requests[0] },
		"duplicate-id":          func(p *checkpointPayload) { p.Requests[1].Update.RequestID = p.Requests[0].Update.RequestID },
		"cas":                   func(p *checkpointPayload) { p.Requests[1].Update.ExpectedRevision = 0 },
		"revision-gap":          func(p *checkpointPayload) { p.Requests[1].Revision = 4 },
		"invalid-worker":        func(p *checkpointPayload) { p.Requests[0].Update.Workers[0].Origin = "invalid" },
		"missing-term":          func(p *checkpointPayload) { p.Position.Term = 0 },
		"index-behind-revision": func(p *checkpointPayload) { p.Position.Index = 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			var p checkpointPayload
			if err := json.Unmarshal(data[12:], &p); err != nil {
				t.Fatal(err)
			}
			mutate(&p)
			payload, _ := json.Marshal(p)
			machine, _, err := RestoreCheckpoint(checkpointFrame(payload))
			if err == nil || machine != nil {
				t.Fatal("accepted semantic corruption")
			}
		})
	}
}

func TestCheckpointGenesisPositionsAndBounds(t *testing.T) {
	for _, position := range []CheckpointPosition{{}, {Index: 8, Term: 7}} {
		data, err := NewMachine().Checkpoint(position)
		if err != nil {
			t.Fatal(err)
		}
		m, got, err := RestoreCheckpoint(data)
		if err != nil || got != position || m.Snapshot().Revision != 0 {
			t.Fatal(got, err)
		}
	}
	for _, position := range []CheckpointPosition{{Index: 1}, {Term: 1}, {Index: 1, Term: 1}} {
		if _, err := checkpointMachine(t).Checkpoint(position); err == nil {
			t.Fatal("accepted invalid position", position)
		}
	}
	m := NewMachine()
	for i := 0; i < maxCheckpointRequests; i++ {
		if _, err := m.Apply(Update{RequestID: fmt.Sprintf("key-%d", i), ExpectedRevision: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	data, err := m.Checkpoint(CheckpointPosition{Index: 1024, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RestoreCheckpoint(data); err != nil {
		t.Fatal("request bound not restorable", err)
	}
	if _, err := m.Apply(Update{RequestID: "too-many", ExpectedRevision: 1024}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Checkpoint(CheckpointPosition{Index: 1025, Term: 1}); err == nil {
		t.Fatal("unbounded successful history")
	}
	oversized := NewMachine()
	u := update("oversized", 0, true)
	u.Workers[0].Origin = "http://" + strings.Repeat("a", maxCheckpointPayload)
	if _, err := oversized.Apply(u); err != nil {
		t.Fatal(err)
	}
	if _, err := oversized.Checkpoint(CheckpointPosition{Index: 1, Term: 1}); err == nil {
		t.Fatal("unbounded raw payload")
	}
}

func TestSeededCheckpointTailMatchesFullReplay(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			full, restored := NewMachine(), NewMachine()
			for i := 0; i < 120; i++ {
				u := update(fmt.Sprintf("request-%d", rng.Intn(25)), uint64(rng.Intn(12)), rng.Intn(2) == 0)
				if i%3 == 0 {
					u.ExpectedRevision = full.Snapshot().Revision
				}
				a, ea := full.Apply(u)
				b, eb := restored.Apply(u)
				if !reflect.DeepEqual(a, b) || (ea == nil) != (eb == nil) || errors.Is(ea, ErrConflict) != errors.Is(eb, ErrConflict) || !reflect.DeepEqual(full.Snapshot(), restored.Snapshot()) {
					t.Fatalf("tail divergence at command %d", i)
				}
				if i%11 == 0 {
					position := CheckpointPosition{Index: uint64(i) + 1, Term: uint64(i/20) + 1}
					frame, err := restored.Checkpoint(position)
					if err != nil {
						t.Fatal(err)
					}
					restored, _, err = RestoreCheckpoint(frame)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			// Comparing the entire canonical image verifies retry history, not only the
			// public configuration view, after every sampled restore/tail schedule.
			position := CheckpointPosition{Index: 120, Term: 6}
			a, ea := full.Checkpoint(position)
			b, eb := restored.Checkpoint(position)
			if ea != nil || eb != nil || !bytes.Equal(a, b) {
				t.Fatal("successful retry table diverged", ea, eb)
			}
		})
	}
}
