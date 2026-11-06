// Command replicationdemo models message delivery with actual local Raft journals.
// It does not run network peers or replace the serving controller.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Kaif10/inference-fleet/internal/control"
	"github.com/Kaif10/inference-fleet/internal/raft"
)

func main() {
	output := flag.String("output", "artifacts/replication/result.json", "transcript")
	flag.Parse()
	if err := run(*output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(output string) error {
	if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(filepath.Dir(output), "replica-journals-")
	if err != nil {
		return err
	}
	ids := []string{"a", "b", "c"}
	nodes := map[string]*raft.Node{}
	stores := map[string]*raft.FileStorage{}
	machines := map[string]*control.Machine{}
	defer func() {
		for _, s := range stores {
			s.Close()
		}
	}()
	open := func() error {
		for i, id := range ids {
			s, err := raft.OpenStorage(filepath.Join(dir, id+".wal"))
			if err != nil {
				return err
			}
			stores[id] = s
			n, err := raft.NewReplica(raft.Config{ID: id, Members: ids, ElectionTicks: 5, HeartbeatTicks: 1, Seed: int64(i + 1)}, s)
			if err != nil {
				return err
			}
			nodes[id] = n
			machines[id] = control.NewMachine()
		}
		return nil
	}
	if err := open(); err != nil {
		return err
	}
	partition := false
	var queue []raft.Message
	deliver := func() error {
		for steps := 0; len(queue) > 0; steps++ {
			if steps > 10000 {
				return fmt.Errorf("message loop did not settle")
			}
			m := queue[0]
			queue = queue[1:]
			if partition && ((m.From == "a") != (m.To == "a")) {
				continue
			}
			out, err := nodes[m.To].Step(m)
			if err != nil {
				return err
			}
			queue = append(queue, out...)
		}
		return nil
	}
	apply := func() error {
		for _, id := range ids {
			entries, err := nodes[id].DrainCommitted()
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if entry.Noop {
					continue
				}
				var command control.Update
				if err := json.Unmarshal(entry.Data, &command); err != nil {
					return err
				}
				if command.RequestID == "orphan" {
					return fmt.Errorf("uncommitted command reached application")
				}
				if _, err := machines[id].Apply(command); err != nil {
					return err
				}
			}
		}
		return nil
	}
	snapshot := func() map[string]any {
		out := map[string]any{}
		for _, id := range ids {
			out[id] = map[string]any{"status": nodes[id].Status(), "commit_index": nodes[id].CommitIndex(), "log": nodes[id].Log(), "config": machines[id].Snapshot()}
		}
		return out
	}
	tick := func(id string) error {
		out, err := nodes[id].Tick()
		if err != nil {
			return err
		}
		queue = append(queue, out...)
		return deliver()
	}
	elect := func(id string) error {
		for i := 0; i < 10 && nodes[id].Status().Role != raft.Leader; i++ {
			if err := tick(id); err != nil {
				return err
			}
		}
		if nodes[id].Status().Role != raft.Leader {
			return fmt.Errorf("no leader")
		}
		return nil
	}
	propose := func(id, key string, revision uint64, drain bool) error {
		data, _ := json.Marshal(control.Update{RequestID: key, ExpectedRevision: revision, Workers: []control.Worker{{ID: "worker-a", Origin: "http://127.0.0.1:18081", Draining: drain}}})
		out, err := nodes[id].Propose(data)
		if err != nil {
			return err
		}
		queue = append(queue, out...)
		return deliver()
	}
	if err := elect("a"); err != nil {
		return err
	}
	if err := propose("a", "drain", 0, true); err != nil {
		return err
	}
	if err := apply(); err != nil {
		return err
	}
	transcript := map[string]any{"kind": "deterministic transport model with actual log journals and configuration machines", "journal_directory": dir, "initial_commit": snapshot()}
	partition = true
	if err := propose("a", "orphan", 1, false); err != nil {
		return err
	}
	if nodes["a"].CommitIndex() != 2 {
		return fmt.Errorf("minority committed")
	}
	for i := 0; i < 30; i++ {
		for _, id := range ids {
			if err := tick(id); err != nil {
				return err
			}
		}
	}
	leader := ""
	for _, id := range []string{"b", "c"} {
		if nodes[id].Status().Role == raft.Leader {
			leader = id
		}
	}
	if leader == "" {
		return fmt.Errorf("majority unavailable")
	}
	if err := propose(leader, "resume", 1, false); err != nil {
		return err
	}
	if err := apply(); err != nil {
		return err
	}
	transcript["partitioned"] = snapshot()
	partition = false
	for i := 0; i < 4; i++ {
		if err := tick(leader); err != nil {
			return err
		}
	}
	if err := apply(); err != nil {
		return err
	}
	for _, id := range ids {
		if nodes[id].CommitIndex() != 4 || machines[id].Snapshot().Revision != 2 || machines[id].Snapshot().Workers[0].Draining {
			return fmt.Errorf("healed state differs")
		}
	}
	transcript["healed"] = snapshot()
	for _, s := range stores {
		s.Close()
	}
	if err := open(); err != nil {
		return err
	}
	queue = nil
	for _, id := range ids {
		if nodes[id].CommitIndex() != 0 {
			return fmt.Errorf("commit watermark unexpectedly durable")
		}
	}
	if err := elect("c"); err != nil {
		return err
	}
	if err := apply(); err != nil {
		return err
	}
	for _, id := range ids {
		if nodes[id].CommitIndex() != 5 || machines[id].Snapshot().Revision != 2 {
			return fmt.Errorf("replay after reopen differs")
		}
	}
	transcript["reopened_and_recommitted"] = snapshot()
	transcript["passed"] = true
	data, err := json.MarshalIndent(transcript, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(output, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Println("PASS: durable replication, minority exclusion, conflict repair, committed config replay; transcript:", output)
	return nil
}
