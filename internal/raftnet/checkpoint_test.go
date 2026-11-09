package raftnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/checkpoint"
	"github.com/Kaif10/distributed-llm-serving/internal/control"
	"github.com/Kaif10/distributed-llm-serving/internal/raft"
)

func checkpointFixture(t *testing.T) (*raft.Node, *checkpoint.Store, []byte) {
	t.Helper()
	dir := t.TempDir()
	storage, err := raft.OpenStorage(filepath.Join(dir, "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })
	update := control.Update{RequestID: "initial", Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}}
	data, _ := json.Marshal(configurationCommand{Version: 1, Kind: "update", Update: &update})
	if err := storage.Save(raft.HardState{Term: 1, VotedFor: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SaveLog([]raft.Entry{{Index: 1, Term: 1, Noop: true}, {Index: 2, Term: 1, Data: data}}); err != nil {
		t.Fatal(err)
	}
	node, err := raft.NewReplica(raft.Config{ID: "a", Members: []string{"a", "b", "c"}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 7}, storage)
	if err != nil {
		t.Fatal(err)
	}
	publications, err := checkpoint.Open(filepath.Join(dir, "checkpoint.wal"), checkpoint.Identity{Group: "test", Member: "a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { publications.Close() })
	machine := control.NewMachine()
	if _, err := machine.Apply(update); err != nil {
		t.Fatal(err)
	}
	frame, err := machine.Checkpoint(control.CheckpointPosition{Index: 2, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	return node, publications, frame
}

func TestCheckpointRecoveryWaitsForCommitThenAppliesSuffix(t *testing.T) {
	node, store, frame := checkpointFixture(t)
	if _, err := store.Publish(frame); err != nil {
		t.Fatal(err)
	}
	host, err := NewConfigurationWithCheckpoints(node, map[string]string{}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.applyCommitted(); err != nil {
		t.Fatal(err)
	}
	if host.applied != 0 || host.machine.Snapshot().Revision != 0 || !host.checkpointStatus.RecoveryPending {
		t.Fatal("exposed selected image before commitment")
	}
	if result, err := host.captureCheckpoint(); err != nil || result.Code != 503 {
		t.Fatal("captured pending/uncommitted state", err)
	}
	if _, err := node.Step(raft.Message{Type: raft.AppendEntries, From: "b", To: "a", Term: 2, PrevIndex: 2, PrevTerm: 1, LeaderCommit: 2, Probe: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := host.applyCommitted(); err != nil {
		t.Fatal(err)
	}
	if host.applied != 2 || host.machine.Snapshot().Revision != 1 || host.checkpointStatus.RecoveryPending || host.checkpointStatus.RecoveryIndex != 2 {
		t.Fatal("selected committed prefix not installed")
	}
	update := control.Update{RequestID: "resume", ExpectedRevision: 1, Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081"}}}
	data, _ := json.Marshal(configurationCommand{Version: 1, Kind: "update", Update: &update})
	if _, err := node.Step(raft.Message{Type: raft.AppendEntries, From: "b", To: "a", Term: 2, PrevIndex: 2, PrevTerm: 1, Entries: []raft.Entry{{Index: 3, Term: 2, Data: data}}, LeaderCommit: 3, Probe: 2}); err != nil {
		t.Fatal(err)
	}
	results, err := host.applyCommitted()
	if err != nil || results[3].Code != 200 || host.machine.Snapshot().Revision != 2 {
		t.Fatal("suffix replay failed", err)
	}
	captured, err := host.captureCheckpoint()
	if err != nil || captured.Code != 200 || captured.Checkpoint.Published.Generation != 2 || captured.Checkpoint.Published.Position.Index != 3 {
		t.Fatal(captured, err)
	}
	captured.Checkpoint.Published.Position.Index = 999
	if host.checkpointSnapshot().Published.Position.Index != 3 {
		t.Fatal("response aliases live checkpoint diagnostics")
	}
	store.Close()
	if _, err := host.captureCheckpoint(); !errors.Is(err, checkpoint.ErrUnavailable) {
		t.Fatal("acknowledged unavailable publication", err)
	}
}

func TestCheckpointRejectsWrongBoundaryAndSameTermDifferentHistory(t *testing.T) {
	for _, mode := range []string{"index", "term", "history", "member"} {
		t.Run(mode, func(t *testing.T) {
			node, store, frame := checkpointFixture(t)
			machine, position, err := control.RestoreCheckpoint(frame)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "index":
				position.Index = 3
			case "term":
				position.Term = 2
			case "history":
				machine = control.NewMachine()
				if _, err := machine.Apply(control.Update{RequestID: "different", Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}}); err != nil {
					t.Fatal(err)
				}
			case "member":
				wrong, err := checkpoint.Open(filepath.Join(t.TempDir(), "wrong.wal"), checkpoint.Identity{Group: "test", Member: "b"})
				if err != nil {
					t.Fatal(err)
				}
				defer wrong.Close()
				store = wrong
			}
			frame, err = machine.Checkpoint(position)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Publish(frame); err != nil {
				t.Fatal(err)
			}
			if host, err := NewConfigurationWithCheckpoints(node, map[string]string{}, store); err == nil || host != nil {
				t.Fatal("accepted invalid checkpoint binding")
			}
		})
	}
}

func TestCheckpointRevalidatesPrefixWhenCommitReturns(t *testing.T) {
	node, store, frame := checkpointFixture(t)
	if _, err := store.Publish(frame); err != nil {
		t.Fatal(err)
	}
	host, err := NewConfigurationWithCheckpoints(node, map[string]string{}, store)
	if err != nil {
		t.Fatal(err)
	}
	// A deliberately inconsistent recovered checkpoint must not be installed if
	// its initially unconfirmed journal prefix changes before commitment returns.
	update := control.Update{RequestID: "replacement", Workers: []control.Worker{}}
	data, _ := json.Marshal(configurationCommand{Version: 1, Kind: "update", Update: &update})
	_, err = node.Step(raft.Message{Type: raft.AppendEntries, From: "b", To: "a", Term: 2, Entries: []raft.Entry{{Index: 1, Term: 2, Noop: true}, {Index: 2, Term: 2, Data: data}}, LeaderCommit: 2, Probe: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.applyCommitted(); err == nil || host.applied != 0 || host.machine.Snapshot().Revision != 0 {
		t.Fatal("installed checkpoint after binding changed")
	}
}

func TestHTTPCheckpointCaptureIsOwnerSerializedAndIdempotent(t *testing.T) {
	node, _ := committedLeader(t, []string{"a"})
	store, err := checkpoint.Open(filepath.Join(t.TempDir(), "checkpoint.wal"), checkpoint.Identity{Group: "test", Member: "a"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	host, err := NewConfigurationWithCheckpoints(node, map[string]string{"a": "http://127.0.0.1:1"}, store)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Run(ctx) }()
	server := httptest.NewServer(host.ClientHandler())
	defer func() {
		cancel()
		server.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	update := control.Update{RequestID: "first", Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}}
	if code, state := configRequest(t, server.URL, "PUT", &update); code != 200 || state.Revision != 1 {
		t.Fatal(code, state)
	}
	for i := 0; i < 2; i++ {
		response, err := http.Post(server.URL+"/checkpoint", "application/octet-stream", nil)
		if err != nil {
			t.Fatal(err)
		}
		var status CheckpointStatus
		err = json.NewDecoder(response.Body).Decode(&status)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || status.Published.Generation != 1 || status.Published.Position.Index != 2 {
			t.Fatal("bad capture", response.StatusCode, status, err)
		}
	}
	response, err := http.Post(server.URL+"/checkpoint", "application/json", bytes.NewBufferString("{}"))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatal("capture accepted unexpected input")
	}
	// Repeated concurrent diagnostics and captures exercise independent response
	// metadata under the owner; the race build checks marshaling after delivery.
	for i := 0; i < 5; i++ {
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			r, err := http.Get(server.URL + "/status")
			if err == nil {
				r.Body.Close()
			}
		}()
		r, err := http.Post(server.URL+"/checkpoint", "text/plain", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatal("diagnostic request blocked")
		}
	}
}
