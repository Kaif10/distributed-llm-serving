package checkpoint

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Kaif10/inference-fleet/internal/control"
)

var testIdentity = Identity{Group: "test-fleet", Member: "a"}

func image(t *testing.T, count int, index uint64) []byte {
	t.Helper()
	m := control.NewMachine()
	for i := 0; i < count; i++ {
		_, err := m.Apply(control.Update{RequestID: fmt.Sprintf("request-%d", i), ExpectedRevision: uint64(i), Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: i%2 == 0}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := m.Checkpoint(control.CheckpointPosition{Index: index, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func seed(t *testing.T) (string, []byte, []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "published.wal")
	first, second := image(t, 1, 2), image(t, 2, 3)
	store, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, ok, err := store.Latest(); err != nil || ok {
		t.Fatal("empty store selected checkpoint", err)
	}
	if _, err := store.Publish(first); err != nil {
		t.Fatal(err)
	}
	return path, first, second
}
func TestPublishReopenIdempotencyAndCallerOwnership(t *testing.T) {
	path, first, second := seed(t)
	s, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	if other, err := Open(path, testIdentity); err == nil {
		other.Close()
		t.Fatal("accepted another owner")
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := s.Publish(first)
	if err != nil || repeated.Generation != 1 {
		t.Fatal(repeated, err)
	}
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Fatal("idempotent retry appended record")
	}
	want := bytes.Clone(second)
	got, err := s.Publish(second)
	if err != nil || got.Generation != 2 {
		t.Fatal(got, err)
	}
	second[20] ^= 1
	got.Data[20] ^= 1
	selected, ok, err := s.Latest()
	if err != nil || !ok || !bytes.Equal(selected.Data, want) {
		t.Fatal("caller mutated store", err)
	}
	selected.Data[20] ^= 1
	s.Close()
	s, err = Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	selected, ok, err = s.Latest()
	if err != nil || !ok || selected.Generation != 2 || !bytes.Equal(selected.Data, want) {
		t.Fatal("reopen lost selected generation", err)
	}
	restored, _, err := control.RestoreCheckpoint(selected.Data)
	if err != nil {
		t.Fatal(err)
	}
	original, err := restored.Apply(control.Update{RequestID: "request-0", Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}})
	if err != nil || original.Revision != 1 || !original.Workers[0].Draining || restored.Snapshot().Revision != 2 {
		t.Fatal("lost retry history", err)
	}
}

func TestPublicationRejectsIdentityRegressionAndForks(t *testing.T) {
	path, first, second := seed(t)
	if s, err := Open(path, Identity{Group: "other", Member: "a"}); err == nil {
		s.Close()
		t.Fatal("accepted other group")
	}
	if s, err := Open(path, Identity{Group: testIdentity.Group, Member: "b"}); err == nil {
		s.Close()
		t.Fatal("accepted other member")
	}
	s, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Publish(second); err != nil {
		t.Fatal(err)
	}
	fork := control.NewMachine()
	for i := 0; i < 3; i++ {
		if _, err := fork.Apply(control.Update{RequestID: fmt.Sprintf("request-%d", i), ExpectedRevision: uint64(i), Workers: []control.Worker{}}); err != nil {
			t.Fatal(err)
		}
	}
	forkImage, err := fork.Checkpoint(control.CheckpointPosition{Index: 4, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	sameIndex := image(t, 3, 3)
	oldHistory := image(t, 1, 4)
	for _, data := range [][]byte{first, sameIndex, forkImage, oldHistory, []byte("invalid")} {
		before, _ := os.Stat(path)
		if _, err := s.Publish(data); err == nil {
			t.Fatal("accepted invalid advance")
		}
		after, _ := os.Stat(path)
		selected, ok, err := s.Latest()
		if err != nil || !ok || selected.Generation != 2 || before.Size() != after.Size() {
			t.Fatal("rejection changed/fenced publication", err)
		}
	}
}

func TestPartialAppendAndSyncFailureFenceUntilRecovery(t *testing.T) {
	for _, mode := range []string{"partial-write", "short-write", "sync"} {
		t.Run(mode, func(t *testing.T) {
			path, first, second := seed(t)
			s, err := Open(path, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "sync" {
				s.syncFile = func() error { return errors.New("injected sync failure") }
			} else {
				s.writeFrame = func(frame []byte) (int, error) {
					n, err := s.file.Write(frame[:len(frame)/2])
					if err != nil {
						return n, err
					}
					if mode == "short-write" {
						return n, nil
					}
					return n, errors.New("injected write failure")
				}
			}
			if _, err := s.Publish(second); !errors.Is(err, ErrUnavailable) {
				t.Fatal("failure acknowledged", err)
			}
			if _, _, err := s.Latest(); !errors.Is(err, ErrUnavailable) {
				t.Fatal("fenced store returned cached state", err)
			}
			if _, err := s.Publish(first); !errors.Is(err, ErrUnavailable) {
				t.Fatal("fenced store accepted retry", err)
			}
			s.Close()
			s, err = Open(path, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			selected, ok, err := s.Latest()
			if err != nil || !ok {
				t.Fatal(err)
			}
			expected := uint64(1)
			if mode == "sync" {
				expected = 2
			}
			if selected.Generation != expected {
				t.Fatal("wrong recovery selection", selected.Generation)
			}
			retried, err := s.Publish(second)
			if err != nil || retried.Generation != 2 {
				t.Fatal("uncertain retry did not converge", err)
			}
		})
	}
}

func TestRecoveryBarrierAndCompleteCorruptionFailClosed(t *testing.T) {
	path, _, second := seed(t)
	if s, err := open(path, testIdentity, func(*os.File) error { return errors.New("recovery sync failed") }); err == nil {
		s.Close()
		t.Fatal("recovery exposed data before sync")
	}
	s, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(second); err != nil {
		t.Fatal(err)
	}
	s.Close()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents[len(contents)-1] ^= 1
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path, testIdentity); err == nil {
		s.Close()
		t.Fatal("fell back from corrupt complete publication")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(contents, after) {
		t.Fatal("corruption was silently trimmed", err)
	}
}

func TestIncompleteFinalHeaderAndPayloadRecoverPriorGeneration(t *testing.T) {
	for _, cut := range []int{1, 7, 12, 25, 100} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			path, _, second := seed(t)
			base, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, position, err := control.RestoreCheckpoint(second)
			if err != nil {
				t.Fatal(err)
			}
			frame, err := frameRecord(record{Kind: "fleet-publication-v1", Selection: Selection{Generation: 2, Identity: testIdentity, Position: position, SHA256: digest(second)}, Data: second})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(bytes.Clone(base), frame[:cut]...), 0600); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			selected, ok, err := s.Latest()
			if err != nil || !ok || selected.Generation != 1 {
				t.Fatal("incomplete append displaced publication", err)
			}
			info, _ := os.Stat(path)
			if info.Size() != int64(len(base)) {
				t.Fatal("incomplete tail not repaired")
			}
		})
	}
}

func TestPublicationBoundDoesNotEvictOrBlockIdenticalRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.wal")
	s, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var last []byte
	for i := 0; i < maxGenerations; i++ {
		last = image(t, 1, uint64(i)+2)
		if selected, err := s.Publish(last); err != nil || selected.Generation != uint64(i)+1 {
			t.Fatal(err)
		}
	}
	if _, err := s.Publish(image(t, 1, 20)); !errors.Is(err, ErrFull) {
		t.Fatal("unbounded journal", err)
	}
	selected, err := s.Publish(last)
	if err != nil || selected.Generation != maxGenerations {
		t.Fatal("identical retry failed at bound", err)
	}
	if _, ok, err := s.Latest(); err != nil || !ok {
		t.Fatal("capacity failure fenced known state", err)
	}
	s.Close()
	if _, err := s.Publish(last); !errors.Is(err, ErrUnavailable) {
		t.Fatal("closed publisher accepted write", err)
	}
}

func TestRecoveryRejectsCompleteInvalidMetadata(t *testing.T) {
	for _, mode := range []string{"generation-gap", "position-mismatch", "digest-mismatch", "unknown-version"} {
		t.Run(mode, func(t *testing.T) {
			path, _, second := seed(t)
			base, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, position, err := control.RestoreCheckpoint(second)
			if err != nil {
				t.Fatal(err)
			}
			r := record{Kind: "fleet-publication-v1", Selection: Selection{Generation: 2, Identity: testIdentity, Position: position, SHA256: digest(second)}, Data: second}
			switch mode {
			case "generation-gap":
				r.Generation = 3
			case "position-mismatch":
				r.Position.Index++
			case "digest-mismatch":
				r.SHA256 = "wrong"
			case "unknown-version":
				r.Kind = "fleet-publication-v2"
			}
			frame, err := frameRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(bytes.Clone(base), frame...), 0600); err != nil {
				t.Fatal(err)
			}
			if s, err := Open(path, testIdentity); err == nil {
				s.Close()
				t.Fatal("selected/fell back from invalid complete metadata")
			}
			info, _ := os.Stat(path)
			if info.Size() != int64(len(base)+len(frame)) {
				t.Fatal("complete invalid record was trimmed")
			}
		})
	}
}

func TestUnreferencedCheckpointFileDoesNotAffectSelection(t *testing.T) {
	path, _, second := seed(t)
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "newer-unpublished.checkpoint"), second, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	selected, ok, err := s.Latest()
	if err != nil || !ok || selected.Generation != 1 {
		t.Fatal("unreferenced file replaced publication", err)
	}
}

func TestConcurrentSelectionAndPublication(t *testing.T) {
	path, _, _ := seed(t)
	store, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var readers sync.WaitGroup
	failures := make(chan error, 4)
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 50; j++ {
				selected, ok, err := store.Latest()
				if err != nil || !ok {
					failures <- fmt.Errorf("selection failed: %v", err)
					return
				}
				if _, _, err := control.RestoreCheckpoint(selected.Data); err != nil {
					failures <- err
					return
				}
				selected.Data[0] ^= 1 // An independent caller buffer, including during another publication.
			}
		}()
	}
	for i := 2; i <= 6; i++ {
		if _, err := store.Publish(image(t, i, uint64(i)+1)); err != nil {
			t.Fatal(err)
		}
	}
	readers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	selected, ok, err := store.Latest()
	if err != nil || !ok || selected.Generation != 6 {
		t.Fatal("concurrent readers changed selection", err)
	}
}
