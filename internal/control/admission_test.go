package control

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCheckpointAdmissionCountBoundaryAndHistoricalRetry(t *testing.T) {
	m := NewMachine()
	first := Update{RequestID: "first", Workers: []Worker{{ID: "original", Origin: "http://127.0.0.1:10000", Draining: true}}}
	if _, err := m.Apply(first); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < maxCheckpointRequests-1; i++ {
		if _, err := m.Apply(Update{RequestID: fmt.Sprint(i), ExpectedRevision: uint64(i), Workers: []Worker{}}); err != nil {
			t.Fatal(err)
		}
	}
	last := Update{RequestID: "last", ExpectedRevision: 1023, Workers: []Worker{}}
	if _, err := m.ApplyCheckpointable(last); err != nil {
		t.Fatal("last allowed success rejected", err)
	}
	position := CheckpointPosition{Index: 2000, Term: 3}
	before, err := m.Checkpoint(position)
	if err != nil {
		t.Fatal(err)
	}
	denied := Update{RequestID: "over-capacity", ExpectedRevision: 1024, Workers: []Worker{}}
	if err := m.CheckCheckpointAdmission(denied); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if _, err := m.ApplyCheckpointable(denied); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	after, err := m.Checkpoint(position)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejection mutated state/history", err)
	}
	if err := m.CheckCheckpointAdmission(first); err != nil {
		t.Fatal("historical retry rejected", err)
	}
	retried, err := m.ApplyCheckpointable(first)
	if err != nil || retried.Revision != 1 || !retried.Workers[0].Draining || m.Snapshot().Revision != 1024 {
		t.Fatal(retried, err)
	}
	restored, _, err := RestoreCheckpoint(after)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := restored.ApplyCheckpointable(first); err != nil || got.Revision != 1 {
		t.Fatal("restoration lost retry", got, err)
	}
	// Legacy commands retain their original semantics when replayed after upgrade.
	if _, err := m.Apply(denied); err != nil || m.Snapshot().Revision != 1025 {
		t.Fatal("legacy Apply changed", err)
	}
}

func TestCheckpointAdmissionByteLimitWithoutMutation(t *testing.T) {
	workers := make([]Worker, 128)
	for i := range workers {
		workers[i] = Worker{ID: fmt.Sprintf("%03d", i) + strings.Repeat("x", 125), Origin: fmt.Sprintf("http://127.0.0.1:%d", 10000+i)}
	}
	m := NewMachine()
	for i := 0; i < 250; i++ {
		if _, err := m.Apply(Update{RequestID: fmt.Sprint(i), ExpectedRevision: uint64(i), Workers: workers}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 250; i < 500; i++ {
		u := Update{RequestID: fmt.Sprint(i), ExpectedRevision: uint64(i), Workers: workers}
		_, err := m.ApplyCheckpointable(u)
		if errors.Is(err, ErrCapacity) {
			if m.Snapshot().Revision != uint64(i) {
				t.Fatal("byte rejection changed revision")
			}
			if _, err := m.Checkpoint(CheckpointPosition{Index: ^uint64(0), Term: ^uint64(0)}); err != nil {
				t.Fatal("prior success not checkpointable", err)
			}
			t.Log("byte admission reached at successful request count", i)
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("large valid history exceeded byte budget without rejection")
}
