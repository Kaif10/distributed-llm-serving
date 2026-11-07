package checkpoint

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// This helper is deliberately killed while blocked inside a real publication.
// Fault controls exist only in this test binary, not in a serving API or CLI.
func TestPublicationProcessHelper(t *testing.T) {
	stage := os.Getenv("FLEET_CP_HELPER_STAGE")
	if stage == "" {
		t.Skip("child process only")
	}
	path, ready := os.Getenv("FLEET_CP_HELPER_PATH"), os.Getenv("FLEET_CP_HELPER_READY")
	store, err := Open(path, testIdentity)
	if stage == "lock-probe" {
		if err != nil {
			os.Exit(0)
		}
		store.Close()
		os.Exit(3)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	park := func() {
		if err := os.WriteFile(ready, []byte(stage), 0600); err != nil {
			t.Fatal(err)
		}
		// Keep a live deadline so Go's deadlock detector cannot exit the helper
		// before the parent kills it at the staged publication boundary.
		<-time.After(30 * time.Second)
		t.Fatal("parent did not kill staged publication helper")
	}
	switch stage {
	case "partial":
		store.writeFrame = func(frame []byte) (int, error) {
			n, err := store.file.Write(frame[:len(frame)/2])
			if err != nil {
				t.Fatal(err)
			}
			park()
			return n, nil
		}
	case "before-sync":
		store.afterAppend = park
	case "after-sync":
		store.afterSync = park
	case "acknowledged":
	default:
		t.Fatal("unknown child stage")
	}
	if _, err := store.Publish(image(t, 2, 3)); err != nil {
		t.Fatal(err)
	}
	if stage != "acknowledged" {
		t.Fatal("publication passed fault boundary")
	}
	park()
}

func childCommand(t *testing.T, stage, path, ready string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestPublicationProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "FLEET_CP_HELPER_STAGE="+stage, "FLEET_CP_HELPER_PATH="+path, "FLEET_CP_HELPER_READY="+ready)
	return cmd
}
func TestPublicationProcessKillRecovery(t *testing.T) {
	for _, stage := range []string{"partial", "before-sync", "after-sync", "acknowledged"} {
		t.Run(stage, func(t *testing.T) {
			path, _, second := seed(t)
			marker := filepath.Join(filepath.Dir(path), "ready")
			cmd := childCommand(t, stage, path, marker)
			var output bytes.Buffer
			cmd.Stdout = &output
			cmd.Stderr = &output
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
					t.Fatalf("child exited before boundary: %v %s", err, output.String())
				case <-deadline.C:
					t.Fatal("child did not reach publication boundary")
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
				t.Fatal("child was not abruptly terminated")
			}
			store, err := Open(path, testIdentity)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			selected, ok, err := store.Latest()
			if err != nil || !ok {
				t.Fatal(err)
			}
			expected := uint64(2)
			if stage == "partial" {
				expected = 1
			}
			if selected.Generation != expected {
				t.Fatalf("wrong crash recovery selection: stage=%s generation=%d", stage, selected.Generation)
			}
			retried, err := store.Publish(second)
			if err != nil || retried.Generation != 2 {
				t.Fatal("retry failed after crash", err)
			}
			t.Logf("stage=%s selected_generation=%d retry_generation=%d acknowledged_before_kill=%t", stage, selected.Generation, retried.Generation, stage == "acknowledged")
		})
	}
}

func TestPublicationProcessExclusiveOwner(t *testing.T) {
	path, _, _ := seed(t)
	store, err := Open(path, testIdentity)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cmd := childCommand(t, "lock-probe", path, "")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child acquired or could not test held lock: %v %s", err, output)
	}
}
