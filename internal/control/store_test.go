package control

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func update(id string, revision uint64, drain bool) Update {
	return Update{RequestID: id, ExpectedRevision: revision, Workers: []Worker{{ID: "worker-a", Origin: "http://127.0.0.1:18081", Draining: drain}}}
}

func TestConcurrentCASAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	var successes atomic.Int32
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Apply(update(string(rune('A'+i)), 0, true))
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatal("concurrent stale writers committed", successes.Load())
	}
	s.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Read()
	if err != nil || state.Revision != 1 || !state.Workers[0].Draining {
		t.Fatal("acknowledged state lost", state, err)
	}
}

func TestIdempotencySurvivesLaterChangesAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(update("drain", 0, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(update("resume", 1, false)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	state, err := s.Apply(update("drain", 0, true))
	if err != nil || state.Revision != 1 || !state.Workers[0].Draining {
		t.Fatal("retry did not return original result", state, err)
	}
	current, _ := s.Read()
	if current.Revision != 2 || current.Workers[0].Draining {
		t.Fatal("old retry changed latest state")
	}
	if _, err := s.Apply(update("drain", 0, false)); !errors.Is(err, ErrConflict) {
		t.Fatal("reused request ID accepted", err)
	}
	if _, err := s.Apply(update("stale", 0, false)); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision accepted", err)
	}
}

func TestEveryIncompleteTailPreservesPriorRecord(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "source.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(update("first", 0, true)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(update("second", 1, false)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	full, _ := os.ReadFile(path)
	for length := len(first); length < len(full); length++ {
		target := filepath.Join(root, "tail.wal")
		if err := os.WriteFile(target, full[:length], 0600); err != nil {
			t.Fatal(err)
		}
		recovered, err := Open(target)
		if err != nil {
			t.Fatalf("tail length %d: %v", length-len(first), err)
		}
		state, err := recovered.Read()
		if err != nil || state.Revision != 1 || !state.Workers[0].Draining {
			recovered.Close()
			t.Fatal("prior state lost", state, err)
		}
		if _, err := recovered.Apply(update("replacement", 1, false)); err != nil {
			t.Fatal("cannot append after repair", err)
		}
		recovered.Close()
	}
}

func TestCorruptionAndSecondWriterRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(path); err == nil {
		other.Close()
		t.Fatal("second writer acquired journal")
	}
	if _, err := s.Apply(update("first", 0, true)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, position := range []int{0, len(data) - 1} {
		bad := append([]byte{}, data...)
		bad[position] ^= 1
		if err := os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if recovered, err := Open(path); err == nil {
			recovered.Close()
			t.Fatal("corruption silently accepted")
		}
	}
}

func TestSyncErrorFencesStoreAndRetryRecoversUnknownOutcome(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.syncFile = func() error { return errors.New("injected fsync error") }
	if _, err := s.Apply(update("unknown", 0, true)); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := s.Read(); !errors.Is(err, ErrUnavailable) {
		t.Fatal("uncertain state was readable")
	}
	if _, err := s.Apply(update("later", 0, false)); !errors.Is(err, ErrUnavailable) {
		t.Fatal("store continued after uncertain write")
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	state, err := s.Apply(update("unknown", 0, true))
	if err != nil || state.Revision != 1 {
		t.Fatal("unknown complete record was not recovered idempotently", state, err)
	}
}

func TestRecoveryRequiresSyncBeforePublishingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "uncertain.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.syncFile = func() error { return errors.New("uncertain write") }
	if _, err := s.Apply(update("uncertain", 0, true)); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	s.Close()
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	barrierErr := errors.New("recovery sync failed")
	calls := 0
	recovering := &Store{file: f, machine: NewMachine(), syncFile: func() error { calls++; return barrierErr }}
	if err := recovering.recover(); !errors.Is(err, barrierErr) || calls != 1 {
		t.Fatal("recovery published without sync barrier", err, calls)
	}
}
