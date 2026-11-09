// Command checkpointdemo validates offline checkpoint recovery against complete
// replay using one real Raft journal. It never truncates or installs a live log.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Kaif10/distributed-llm-serving/internal/control"
	"github.com/Kaif10/distributed-llm-serving/internal/raft"
)

type command struct {
	Version int            `json:"version"`
	Kind    string         `json:"kind"`
	Update  control.Update `json:"update"`
}

func main() {
	output := flag.String("output", "artifacts/checkpoint/result.json", "offline recovery transcript")
	flag.Parse()
	if err := run(*output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func bindPosition(position control.CheckpointPosition, log []raft.Entry, committed uint64) error {
	if position.Index == 0 || position.Index > committed || position.Index > uint64(len(log)) || log[position.Index-1].Term != position.Term {
		return fmt.Errorf("checkpoint boundary is not confirmed by this committed log")
	}
	return nil
}
func apply(machine *control.Machine, entries []raft.Entry, after uint64) error {
	for _, entry := range entries {
		if entry.Index <= after || entry.Noop {
			continue
		}
		var c command
		if err := json.Unmarshal(entry.Data, &c); err != nil {
			return err
		}
		if c.Version != 1 || c.Kind != "update" {
			return fmt.Errorf("unexpected demo command")
		}
		if _, err := machine.Apply(c.Update); err != nil && !errors.Is(err, control.ErrConflict) {
			return err
		}
	}
	return nil
}
func run(output string) error {
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return err
	}
	workingTree, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(filepath.Dir(output), "checkpoint-demo-")
	if err != nil {
		return err
	}
	journal := filepath.Join(dir, "node.wal")
	checkpointPath := filepath.Join(dir, "application.checkpoint")
	var store *raft.FileStorage
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	open := func() (*raft.Node, error) {
		var err error
		store, err = raft.OpenStorage(journal)
		if err != nil {
			return nil, err
		}
		return raft.NewReplica(raft.Config{ID: "a", Members: []string{"a"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 7}, store)
	}
	elect := func(node *raft.Node) error {
		for i := 0; i < 10 && node.Status().Role != raft.Leader; i++ {
			if _, err := node.Tick(); err != nil {
				return err
			}
		}
		if node.Status().Role != raft.Leader {
			return fmt.Errorf("demo failed to elect")
		}
		return nil
	}
	node, err := open()
	if err != nil {
		return err
	}
	if err := elect(node); err != nil {
		return err
	}
	machine := control.NewMachine()
	consume := func() error {
		entries, err := node.DrainCommitted()
		if err != nil {
			return err
		}
		return apply(machine, entries, 0)
	}
	propose := func(key string, revision uint64, draining bool) error {
		data, err := json.Marshal(command{Version: 1, Kind: "update", Update: control.Update{RequestID: key, ExpectedRevision: revision, Workers: []control.Worker{{ID: "worker-a", Origin: "http://127.0.0.1:18081", Draining: draining}}}})
		if err != nil {
			return err
		}
		if _, err := node.Propose(data); err != nil {
			return err
		}
		return consume()
	}
	if err := consume(); err != nil {
		return err
	}
	if err := propose("drain", 0, true); err != nil {
		return err
	}
	if err := propose("resume", 1, false); err != nil {
		return err
	}
	position := control.CheckpointPosition{Index: node.CommitIndex(), Term: node.Log()[node.CommitIndex()-1].Term}
	data, err := machine.Checkpoint(position)
	if err != nil {
		return err
	}
	// This is a new offline artifact, not atomic replacement of an installed
	// snapshot or a claim about directory fsync/power-loss recovery.
	file, err := os.OpenFile(checkpointPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	checkpointState := machine.Snapshot()
	if err := propose("drain", 0, true); err != nil {
		return err
	}
	if err := propose("stale", 1, true); err != nil {
		return err
	}
	if err := propose("change", 2, true); err != nil {
		return err
	}
	beforeRestart := machine.Snapshot()
	if err := store.Close(); err != nil {
		return err
	}
	store = nil
	node, err = open()
	if err != nil {
		return err
	}
	frame, err := os.ReadFile(checkpointPath)
	if err != nil {
		return err
	}
	restored, loadedPosition, err := control.RestoreCheckpoint(frame)
	if err != nil {
		return err
	}
	if bindPosition(loadedPosition, node.Log(), node.CommitIndex()) == nil {
		return fmt.Errorf("accepted checkpoint before re-establishing commit")
	}
	if err := elect(node); err != nil {
		return err
	}
	if err := bindPosition(loadedPosition, node.Log(), node.CommitIndex()); err != nil {
		return err
	}
	wrong := loadedPosition
	wrong.Term++
	if bindPosition(wrong, node.Log(), node.CommitIndex()) == nil {
		return fmt.Errorf("accepted mismatched checkpoint boundary")
	}
	entries, err := node.DrainCommitted()
	if err != nil {
		return err
	}
	full := control.NewMachine()
	if err := apply(full, entries, 0); err != nil {
		return err
	}
	if err := apply(restored, entries, loadedPosition.Index); err != nil {
		return err
	}
	finalPosition := control.CheckpointPosition{Index: node.CommitIndex(), Term: node.Log()[node.CommitIndex()-1].Term}
	fullImage, err := full.Checkpoint(finalPosition)
	if err != nil {
		return err
	}
	restoredImage, err := restored.Checkpoint(finalPosition)
	if err != nil {
		return err
	}
	if !bytes.Equal(fullImage, restoredImage) {
		return fmt.Errorf("checkpoint plus committed suffix differs from full replay")
	}
	retry := control.Update{RequestID: "drain", ExpectedRevision: 0, Workers: []control.Worker{{ID: "worker-a", Origin: "http://127.0.0.1:18081", Draining: true}}}
	original, err := restored.Apply(retry)
	if err != nil || original.Revision != 1 || !original.Workers[0].Draining {
		return fmt.Errorf("checkpoint lost original retry result: %v", err)
	}
	retry.Workers[0].Draining = false
	if _, err := restored.Apply(retry); !errors.Is(err, control.ErrConflict) {
		return fmt.Errorf("checkpoint accepted altered request ID")
	}
	damaged := bytes.Clone(frame)
	damaged[len(damaged)-1] ^= 1
	if invalid, _, err := control.RestoreCheckpoint(damaged); err == nil || invalid != nil {
		return fmt.Errorf("accepted corrupted checkpoint")
	}
	checksum := sha256.Sum256(frame)
	transcript := map[string]any{
		"source_commit": strings.TrimSpace(string(head)), "working_tree": string(workingTree), "go_version": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH,
		"scope":   "single Raft core, explicit ticks, actual journal/checkpoint files; offline application recovery only",
		"journal": journal, "checkpoint": checkpointPath, "checkpoint_bytes": len(frame), "checkpoint_sha256": hex.EncodeToString(checksum[:]),
		"checkpoint_position": position, "checkpoint_configuration": checkpointState, "before_restart": beforeRestart, "final_position": finalPosition,
		"full_replay": full.Snapshot(), "checkpoint_plus_tail": restored.Snapshot(), "historical_retry": original,
		"rejected_before_commit_reestablished": true, "rejected_wrong_boundary_term": true, "altered_id_rejected": true, "corruption_rejected": true, "full_checkpoint_images_equal": true, "log": node.Log(), "passed": true,
	}
	encoded, err := json.MarshalIndent(transcript, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0600); err != nil {
		return err
	}
	fmt.Printf("PASS: checkpoint index %d/revision %d plus committed suffix equals full replay at index %d; original retry restored; %s\n", position.Index, checkpointState.Revision, finalPosition.Index, output)
	return nil
}
