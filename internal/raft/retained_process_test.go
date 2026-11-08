package raft

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRetainedStorageProcessHelper(t *testing.T) {
	stage := os.Getenv("FLEET_RETAINED_STAGE")
	if stage == "" {
		t.Skip("child process only")
	}
	s, err := OpenRetainedStorage(os.Getenv("FLEET_RETAINED_DIR"), retainedTestID)
	if stage == "lock-probe" {
		if err != nil {
			os.Exit(0)
		}
		s.Close()
		os.Exit(3)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	park := func() {
		if err := os.WriteFile(os.Getenv("FLEET_RETAINED_READY"), []byte(stage), 0600); err != nil {
			t.Fatal(err)
		}
		<-time.After(30 * time.Second)
		t.Fatal("parent did not terminate helper")
	}
	switch stage {
	case "cleared":
		s.afterClear = park
	case "partial-header", "partial-body":
		s.write = func(slot int, data []byte) (int, error) {
			cut := 5
			if stage == "partial-body" {
				cut = len(data) / 2
			}
			n, err := s.slots[slot].WriteAt(data[:cut], 0)
			if err != nil {
				t.Fatal(err)
			}
			park()
			return n, nil
		}
	case "before-sync":
		s.afterWrite = park
	case "after-sync":
		s.afterSync = park
	case "acknowledged":
	default:
		t.Fatal("unknown helper stage")
	}
	if err := s.Retain(RetainedSnapshot{Index: 2, Term: 2, Data: []byte("opaque-prefix")}, 3); err != nil {
		t.Fatal(err)
	}
	if stage != "acknowledged" {
		t.Fatal("publication passed fault barrier")
	}
	park()
}

func retainedChild(t *testing.T, stage, dir, marker string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestRetainedStorageProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "FLEET_RETAINED_STAGE="+stage, "FLEET_RETAINED_DIR="+dir, "FLEET_RETAINED_READY="+marker)
	return cmd
}

func TestRetainedStorageProcessKillRecovery(t *testing.T) {
	for _, stage := range []string{"cleared", "partial-header", "partial-body", "before-sync", "after-sync", "acknowledged"} {
		t.Run(stage, func(t *testing.T) {
			dir, s := seedRetainedStorage(t)
			original := s.image
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(t.TempDir(), "ready")
			cmd := retainedChild(t, stage, dir, marker)
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			stopped := false
			defer func() {
				if !stopped {
					cmd.Process.Kill()
					<-done
				}
			}()
			deadline := time.NewTimer(15 * time.Second)
			defer deadline.Stop()
			poll := time.NewTicker(10 * time.Millisecond)
			defer poll.Stop()
		barrier:
			for {
				select {
				case err := <-done:
					stopped = true
					t.Fatalf("helper exited before fault boundary: %v %s", err, output.String())
				case <-deadline.C:
					t.Fatal("helper did not reach fault boundary")
				case <-poll.C:
					if _, err := os.Stat(marker); err == nil {
						break barrier
					}
				}
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err := <-done
			stopped = true
			if err == nil {
				t.Fatal("helper was not abruptly terminated")
			}
			s, err = OpenRetainedStorage(dir, retainedTestID)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			wantBoundary, wantGeneration := uint64(2), original.Generation+1
			if stage == "cleared" || stage == "partial-header" || stage == "partial-body" {
				wantBoundary, wantGeneration = 0, original.Generation
			}
			if s.image.Snapshot.Index != wantBoundary || s.image.Generation != wantGeneration || s.image.State != original.State {
				t.Fatal("incorrect crash selection", s.image)
			}
			entries, err := s.LoadLog()
			if err != nil || len(entries) != 3-int(wantBoundary) || entries[len(entries)-1].Index != 3 || string(entries[len(entries)-1].Data) != "suffix" {
				t.Fatal("acknowledged suffix lost or mixed with boundary", entries, err)
			}
			// Reuse the interrupted slot after reopening; preserve the durable vote.
			if err := s.Save(HardState{Term: 2, VotedFor: "b"}); err == nil {
				t.Fatal("recovery allowed conflicting vote")
			}
			if err := s.Save(HardState{Term: 3, VotedFor: "a"}); err != nil {
				t.Fatal("could not continue after recovery", err)
			}
			t.Logf("stage=%s recovered_generation=%d boundary=%d suffix_end=3", stage, wantGeneration, wantBoundary)
		})
	}
}

func TestRetainedStorageProcessExclusiveOwner(t *testing.T) {
	dir, s := seedRetainedStorage(t)
	defer s.Close()
	if output, err := retainedChild(t, "lock-probe", dir, "").CombinedOutput(); err != nil {
		t.Fatalf("child acquired or could not test held lock: %v %s", err, output)
	}
}
