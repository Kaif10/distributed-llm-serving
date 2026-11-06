// Command electiondemo runs a deterministic transport model with real vote journals.
// It is not a network deployment or controller failover demonstration.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Kaif10/inference-fleet/internal/raft"
)

func main() {
	output := flag.String("output", "artifacts/elections/result.json", "scenario transcript")
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
	dir, err := os.MkdirTemp(filepath.Dir(output), "vote-journals-")
	if err != nil {
		return err
	}
	ids := []string{"a", "b", "c"}
	nodes := map[string]*raft.Node{}
	stores := map[string]*raft.FileStorage{}
	defer func() {
		for _, s := range stores {
			s.Close()
		}
	}()
	config := func(i int, id string) raft.Config {
		return raft.Config{ID: id, Members: ids, ElectionTicks: 5, HeartbeatTicks: 1, Seed: int64(i + 1)}
	}
	for i, id := range ids {
		s, err := raft.OpenStorage(filepath.Join(dir, id+".wal"))
		if err != nil {
			return err
		}
		stores[id] = s
		n, err := raft.New(config(i, id), s)
		if err != nil {
			return err
		}
		nodes[id] = n
	}
	var queue []raft.Message
	partitioned := false
	deliver := func() error {
		for len(queue) > 0 {
			m := queue[0]
			queue = queue[1:]
			if partitioned && ((m.From == "a") != (m.To == "a")) {
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
	transcript := map[string]any{"kind": "deterministic election model with real file storage", "journal_directory": dir}
	snapshot := func() []raft.Status {
		var result []raft.Status
		for _, id := range ids {
			result = append(result, nodes[id].Status())
		}
		return result
	}
	for tick := 0; tick < 10 && nodes["a"].Status().Role != raft.Leader; tick++ {
		out, err := nodes["a"].Tick()
		if err != nil {
			return err
		}
		queue = append(queue, out...)
		if err := deliver(); err != nil {
			return err
		}
	}
	if nodes["a"].Status().Role != raft.Leader {
		return fmt.Errorf("initial election failed")
	}
	transcript["initial_election"] = snapshot()
	partitioned = true
	for tick := 0; tick < 30; tick++ {
		for _, id := range ids {
			out, err := nodes[id].Tick()
			if err != nil {
				return err
			}
			queue = append(queue, out...)
		}
		if err := deliver(); err != nil {
			return err
		}
	}
	transcript["partitioned"] = snapshot()
	majorityLeader := ""
	for _, id := range []string{"b", "c"} {
		if nodes[id].Status().Role == raft.Leader {
			majorityLeader = id
		}
	}
	if majorityLeader == "" || nodes[majorityLeader].Status().Term <= nodes["a"].Status().Term {
		return fmt.Errorf("majority did not elect a higher-term leader")
	}
	partitioned = false
	out, err := nodes[majorityLeader].Tick()
	if err != nil {
		return err
	}
	queue = append(queue, out...)
	if err := deliver(); err != nil {
		return err
	}
	if nodes["a"].Status().Role != raft.Follower {
		return fmt.Errorf("old leader did not step down after healing")
	}
	transcript["healed"] = snapshot()
	for i, id := range ids {
		before := nodes[id].Status()
		stores[id].Close()
		s, err := raft.OpenStorage(filepath.Join(dir, id+".wal"))
		if err != nil {
			return err
		}
		stores[id] = s
		n, err := raft.New(config(i, id), s)
		if err != nil {
			return err
		}
		nodes[id] = n
		if n.Status().Term != before.Term || n.Status().VotedFor != before.VotedFor {
			return fmt.Errorf("durable state changed on reopen")
		}
	}
	transcript["reopened"] = snapshot()
	transcript["passed"] = true
	data, err := json.MarshalIndent(transcript, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(output, append(data, '\n'), 0600); err != nil {
		return err
	}
	fmt.Println("PASS: majority election, higher-term partition recovery, and durable term/vote reopen; transcript:", output)
	return nil
}
