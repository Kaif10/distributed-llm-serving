package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Kaif10/distributed-llm-serving/internal/control"
)

var retainedTestID = RetainedIdentity{Group: "retained-storage-test", Member: "a"}

func seedRetainedStorage(t *testing.T) (string, *RetainedStorage) {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenRetainedStorage(dir, retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(HardState{Term: 2, VotedFor: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveLog([]Entry{{Index: 1, Term: 1, Noop: true}, {Index: 2, Term: 2, Data: []byte("first")}, {Index: 3, Term: 2, Data: []byte("suffix")}}); err != nil {
		t.Fatal(err)
	}
	return dir, s
}

func TestRetainedStorageApplicationRecoveryAndHistoricalRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenRetainedStorage(dir, retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	machine := control.NewMachine()
	n := replica(t, "a", []string{"a"}, s, 1, 16)
	campaign(t, n)
	updates := []control.Update{
		{RequestID: "drain", Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}},
		{RequestID: "resume", ExpectedRevision: 1, Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081"}}},
	}
	var prefix RetainedSnapshot
	for i, u := range updates {
		data, _ := json.Marshal(u)
		if _, err := n.Propose(data); err != nil {
			t.Fatal(err)
		}
		entries, err := n.DrainCommitted()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if !e.Noop {
				var update control.Update
				if err := json.Unmarshal(e.Data, &update); err != nil {
					t.Fatal(err)
				}
				if _, err := machine.Apply(update); err != nil {
					t.Fatal(err)
				}
			}
		}
		if i == 0 {
			term, _ := n.TermAt(n.CommitIndex())
			position := control.CheckpointPosition{Index: n.CommitIndex(), Term: term}
			image, err := machine.Checkpoint(position)
			if err != nil {
				t.Fatal(err)
			}
			prefix = RetainedSnapshot{Index: position.Index, Term: position.Term, Data: image}
		}
	}
	want := machine.Snapshot()
	// Stop using the old node before changing its backing boundary. The test owner
	// supplies the known committed/applied position, not an external capture claim.
	if err := s.Retain(prefix, n.CommitIndex()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenRetainedStorage(dir, retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n = replica(t, "a", []string{"a"}, s, 2, 16)
	image := n.Snapshot()
	restored, pos, err := control.RestoreCheckpoint(image.Data)
	if err != nil || pos.Index != prefix.Index || pos.Term != prefix.Term {
		t.Fatal(pos, err)
	}
	if n.commit != prefix.Index || len(n.Log()) != 1 || n.Log()[0].Index != 3 {
		t.Fatal("selected boundary and suffix separated", n.commit, n.Log())
	}
	campaign(t, n)
	entries, err := n.DrainCommitted()
	if err != nil || len(entries) != 2 || entries[0].Index != 3 || entries[1].Index != 4 {
		t.Fatal(entries, err)
	}
	for _, e := range entries {
		if !e.Noop {
			var u control.Update
			if err := json.Unmarshal(e.Data, &u); err != nil {
				t.Fatal(err)
			}
			if _, err := restored.Apply(u); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !reflect.DeepEqual(restored.Snapshot(), want) {
		t.Fatal("snapshot plus suffix differs from full application")
	}
	retry, err := restored.Apply(updates[0])
	if err != nil || retry.Revision != 1 || !retry.Workers[0].Draining || restored.Snapshot().Revision != 2 {
		t.Fatal("historical retry result lost", retry, err)
	}
}

func TestRetainedStorageValidationOwnershipAndDiskBound(t *testing.T) {
	dir, s := seedRetainedStorage(t)
	defer s.Close()
	if other, err := OpenRetainedStorage(dir, retainedTestID); err == nil {
		other.Close()
		t.Fatal("second owner accepted")
	}
	for _, image := range []RetainedSnapshot{
		{Index: 2, Term: 1, Data: []byte("wrong term")},
		{Index: 4, Term: 2, Data: []byte("outside log")},
		{Index: 2, Term: 2},
	} {
		if err := s.Retain(image, 3); err == nil {
			t.Fatal("invalid retention accepted", image)
		}
	}
	image := RetainedSnapshot{Index: 2, Term: 2, Data: []byte("opaque-prefix")}
	if err := s.Retain(image, 1); err == nil {
		t.Fatal("unapplied prefix accepted")
	}
	if err := s.Retain(image, 3); err != nil {
		t.Fatal(err)
	}
	image.Data[0] = 'X'
	got, _ := s.LoadSnapshot()
	got.Data[0] = 'Y'
	got, _ = s.LoadSnapshot()
	if string(got.Data) != "opaque-prefix" {
		t.Fatal("application bytes alias caller")
	}
	if err := s.Retain(got, 3); err == nil {
		t.Fatal("repeated boundary accepted")
	}
	if err := s.Save(HardState{Term: 2, VotedFor: "b"}); err == nil {
		t.Fatal("conflicting durable vote accepted")
	}
	if err := s.SaveLog([]Entry{{Index: 1, Term: 2, Noop: true}}); err == nil {
		t.Fatal("relative suffix index accepted")
	}
	// Repeated whole images must not accumulate an append-only lifetime journal.
	for term := uint64(3); term <= 70; term++ {
		if err := s.Save(HardState{Term: term, VotedFor: "a"}); err != nil {
			t.Fatal(err)
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 3 {
		t.Fatal("unexpected store files", files, err)
	}
	var actual, bound int64
	for _, f := range files {
		info, err := f.Info()
		if err != nil {
			t.Fatal(err)
		}
		actual += info.Size()
	}
	frame, _ := retainedFrame(s.image)
	owner, _ := retainedFrame(retainedOwner{Kind: "fleet-retained-owner-v1", Identity: retainedTestID})
	bound = int64(2*len(frame) + len(owner))
	if actual > bound {
		t.Fatal("store grew with lifetime writes", actual, bound)
	}
}

func TestRetainedStoragePartialPublicationAndCompleteCorruption(t *testing.T) {
	dir, s := seedRetainedStorage(t)
	selected := s.selected
	before := s.image
	next := before
	next.Generation++
	next.Snapshot = RetainedSnapshot{Index: 2, Term: 2, Data: []byte("opaque-prefix")}
	next.Log = cloneEntries(before.Log[2:])
	frame, _ := retainedFrame(next)
	s.Close()
	for _, cut := range []int{0, 1, 11, 12, len(frame) - 1, len(frame)} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			copyDir := copyRetainedFiles(t, dir)
			path := filepath.Join(copyDir, fmt.Sprintf("image-%d.wal", 1-selected))
			if err := os.WriteFile(path, frame[:cut], 0600); err != nil {
				t.Fatal(err)
			}
			recovered, err := OpenRetainedStorage(copyDir, retainedTestID)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			want := before
			if cut == len(frame) {
				want = next // Complete but possibly unacknowledged is retained.
			}
			if !reflect.DeepEqual(recovered.image, want) {
				t.Fatal("mixed checkpoint/suffix selection", recovered.image, want)
			}
		})
	}
	for _, offset := range []int{0, len(frame) - 1} {
		t.Run(fmt.Sprintf("corrupt-%d", offset), func(t *testing.T) {
			copyDir := copyRetainedFiles(t, dir)
			bad := bytes.Clone(frame)
			bad[offset] ^= 1
			os.WriteFile(filepath.Join(copyDir, fmt.Sprintf("image-%d.wal", 1-selected)), bad, 0600)
			if recovered, err := OpenRetainedStorage(copyDir, retainedTestID); err == nil {
				recovered.Close()
				t.Fatal("corruption caused rollback to older image")
			}
		})
	}
}

func copyRetainedFiles(t *testing.T, dir string) string {
	t.Helper()
	target := t.TempDir()
	for _, name := range []string{"owner.wal", "image-0.wal", "image-1.wal"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return target
}

func TestRetainedStorageSyncFailureFencesAndRecoveryResyncs(t *testing.T) {
	for _, failure := range []string{"clear", "publication", "short-write"} {
		t.Run(failure, func(t *testing.T) {
			dir, s := seedRetainedStorage(t)
			calls := 0
			s.syncSlot = func(slot int) error {
				calls++
				if (failure == "clear" && calls == 1) || (failure == "publication" && calls == 2) {
					return errors.New("injected sync failure")
				}
				return s.slots[slot].Sync()
			}
			if failure == "short-write" {
				s.write = func(slot int, data []byte) (int, error) { return s.slots[slot].WriteAt(data[:len(data)/2], 0) }
			}
			if err := s.Retain(RetainedSnapshot{Index: 2, Term: 2, Data: []byte("prefix")}, 3); !errors.Is(err, ErrStorage) {
				t.Fatal("uncertain I/O not reported", err)
			}
			if _, err := s.Load(); !errors.Is(err, ErrStorage) {
				t.Fatal("uncertain store remained readable", err)
			}
			if err := s.Save(HardState{Term: 3}); !errors.Is(err, ErrStorage) {
				t.Fatal("uncertain store allowed another vote", err)
			}
			s.Close()
			if recovered, err := openRetainedStorage(dir, retainedTestID, func(int) error { return errors.New("recovery sync failure") }); err == nil {
				recovered.Close()
				t.Fatal("recovery exposed bytes before successful sync")
			}
			s, err := OpenRetainedStorage(dir, retainedTestID)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			image, _ := s.LoadSnapshot()
			want := uint64(0)
			if failure == "publication" {
				want = 2
			}
			if image.Index != want {
				t.Fatal("incorrect uncertain-write recovery", image.Index, want)
			}
		})
	}
}

func TestRetainedStorageIdentityMissingFilesAndBootstrapFailClosed(t *testing.T) {
	dir, s := seedRetainedStorage(t)
	s.Close()
	for _, mutate := range []string{"identity", "missing-owner", "missing-slot", "partial-owner", "foreign-slot", "generation-gap", "conflicting-vote", "discarded-suffix"} {
		t.Run(mutate, func(t *testing.T) {
			copyDir := copyRetainedFiles(t, dir)
			id := retainedTestID
			switch mutate {
			case "identity":
				id.Member = "b"
			case "missing-owner":
				os.Remove(filepath.Join(copyDir, "owner.wal"))
			case "missing-slot":
				os.Remove(filepath.Join(copyDir, "image-0.wal"))
			case "partial-owner":
				os.WriteFile(filepath.Join(copyDir, "owner.wal"), []byte("partial"), 0600)
			case "foreign-slot", "generation-gap":
				path := filepath.Join(copyDir, fmt.Sprintf("image-%d.wal", s.selected))
				image := s.image
				if mutate == "foreign-slot" {
					image.Identity.Member = "b"
				} else {
					image.Generation += 2
				}
				frame, _ := retainedFrame(image)
				os.WriteFile(path, frame, 0600)
			case "conflicting-vote", "discarded-suffix":
				image := s.image
				image.Generation++
				if mutate == "conflicting-vote" {
					image.State.VotedFor = "b"
				} else {
					image.Snapshot = RetainedSnapshot{Index: 2, Term: 2, Data: []byte("opaque-prefix")}
					image.Log = []Entry{}
				}
				frame, _ := retainedFrame(image)
				os.WriteFile(filepath.Join(copyDir, fmt.Sprintf("image-%d.wal", 1-s.selected)), frame, 0600)
			}
			if recovered, err := OpenRetainedStorage(copyDir, id); err == nil {
				recovered.Close()
				t.Fatal("invalid store silently reinitialized or accepted")
			}
		})
	}
}

func TestRetainedStorageUncertainVoteCannotDoubleVoteAfterReopen(t *testing.T) {
	dir, s := seedRetainedStorage(t)
	calls := 0
	s.syncSlot = func(slot int) error {
		calls++
		if calls == 2 {
			return errors.New("injected publication sync failure")
		}
		return s.slots[slot].Sync()
	}
	if err := s.Save(HardState{Term: 3, VotedFor: "a"}); !errors.Is(err, ErrStorage) {
		t.Fatal("uncertain vote not fenced", err)
	}
	s.Close()
	s, err := OpenRetainedStorage(dir, retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	hard, err := s.Load()
	if err != nil || hard != (HardState{Term: 3, VotedFor: "a"}) {
		t.Fatal("complete uncertain vote discarded", hard, err)
	}
	if err := s.Save(HardState{Term: 3, VotedFor: "b"}); err == nil {
		t.Fatal("double vote allowed after uncertain publication")
	}
}

func TestRetainedStorageFullLocalLogCanRestartAfterOfflineRetention(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenRetainedStorage(dir, retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	if err := s.Save(HardState{Term: 2}); err != nil {
		t.Fatal(err)
	}
	entries := make([]Entry, 1024)
	for i := range entries {
		entries[i] = Entry{Index: uint64(i + 1), Term: 2, Noop: true}
	}
	if err := s.SaveLog(entries); err != nil {
		t.Fatal(err)
	}
	n := replica(t, "a", []string{"a"}, s, 1, 16)
	// The fixture is a full local no-op log, not 1024 acknowledged application
	// writes. The test owner supplies its known committed prefix separately.
	var electionErr error
	for i := 0; i < 20 && electionErr == nil; i++ {
		_, electionErr = n.Tick()
	}
	if electionErr == nil || electionErr.Error() != "log exceeds 1024 entries" {
		t.Fatal("full-log restart did not reproduce blocked leader no-op", electionErr)
	}
	image, err := control.NewMachine().Checkpoint(control.CheckpointPosition{Index: 1024, Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Retain(RetainedSnapshot{Index: 1024, Term: 2, Data: image}, 1024); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenRetainedStorage(dir, retainedTestID)
	if err != nil {
		t.Fatal(err)
	}
	n = replica(t, "a", []string{"a"}, s, 2, 16)
	if len(n.Log()) != 0 || n.BaseIndex() != 1024 {
		t.Fatal("prefix not replaced by image")
	}
	campaign(t, n)
	if n.CommitIndex() != 1025 {
		t.Fatal("leader no-op still blocked after offline retention")
	}
	token, _, err := n.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer n.ForgetRead(token)
	if ready, err := n.ReadReady(token); err != nil || !ready {
		t.Fatal("new-term read not ready after retained restart", ready, err)
	}
}
