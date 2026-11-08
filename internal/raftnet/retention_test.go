package raftnet

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Kaif10/inference-fleet/internal/control"
	"github.com/Kaif10/inference-fleet/internal/raft"
)

func retainedHost(t *testing.T, store *raft.RetainedStorage) *Host {
	t.Helper()
	node, err := raft.NewReplica(raft.Config{ID: "a", Members: []string{"a"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1}, store)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewConfigurationWithRetention(node, map[string]string{"a": "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func electRetainedHost(t *testing.T, h *Host) {
	t.Helper()
	for i := 0; i < 20 && h.node.Status().Role != raft.Leader; i++ {
		if _, err := h.node.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if h.node.Status().Role != raft.Leader {
		t.Fatal("election did not complete")
	}
	if _, err := h.applyCommitted(); err != nil {
		t.Fatal(err)
	}
}
func applyRetainedUpdate(t *testing.T, h *Host, u control.Update) {
	t.Helper()
	data, err := json.Marshal(configurationCommand{Version: 1, Kind: "update", Update: &u})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.node.Propose(data); err != nil {
		t.Fatal(err)
	}
	results, err := h.applyCommitted()
	if err != nil || results[h.node.CommitIndex()].Code != 200 {
		t.Fatal(results, err)
	}
}

func TestRetentionOwnerRecoverySuffixAndHistoricalRetry(t *testing.T) {
	dir := t.TempDir()
	id := raft.RetainedIdentity{Group: "configuration", Member: "a"}
	s, err := raft.OpenRetainedStorage(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	h := retainedHost(t, s)
	if r, err := h.retainLocal(); err != nil || r.Code != 503 {
		t.Fatal("uncommitted startup retention accepted", r, err)
	}
	electRetainedHost(t, h)
	first := control.Update{RequestID: "drain", Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}}
	applyRetainedUpdate(t, h, first)
	r, err := h.retainLocal()
	if err != nil || r.Code != 200 || r.Retention.BaseIndex != 2 || len(h.node.Log()) != 0 {
		t.Fatal(r, err)
	}
	r.Retention.BaseIndex = 999
	if h.retention.BaseIndex != 2 {
		t.Fatal("retention result aliases owner metadata")
	}
	again, err := h.retainLocal()
	if err != nil || again.Retention.BaseIndex != 2 {
		t.Fatal("repeated maintenance call failed", again, err)
	}
	applyRetainedUpdate(t, h, control.Update{RequestID: "resume", ExpectedRevision: 1, Workers: []control.Worker{}})
	want := h.machine.Snapshot()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = raft.OpenRetainedStorage(dir, id)
	if err != nil {
		t.Fatal(err)
	}
	h = retainedHost(t, s)
	if h.applied != 2 || h.machine.Snapshot().Revision != 1 || h.retention.RecoveredIndex != 2 {
		t.Fatal("application not installed at durable boundary before run")
	}
	if _, _, err := h.node.BeginRead(); err == nil {
		t.Fatal("recovered follower served a fresh read")
	}
	electRetainedHost(t, h)
	if !reflect.DeepEqual(h.machine.Snapshot(), want) || h.applied != 4 {
		t.Fatal("checkpoint plus suffix differs from full replay", h.machine.Snapshot(), want)
	}
	old, err := h.machine.Apply(first)
	if err != nil || old.Revision != 1 || !old.Workers[0].Draining || h.machine.Snapshot().Revision != 2 {
		t.Fatal("original retry result was not restored", old, err)
	}
	if _, err := h.retainLocal(); err != nil || h.retention.RecoveredIndex != 2 || h.retention.BaseIndex != 4 {
		t.Fatal("repeated retention lost recovery diagnostics", err)
	}
}

func TestRetentionConstructorRejectsInvalidApplicationBinding(t *testing.T) {
	for _, mode := range []string{"corrupt", "position"} {
		t.Run(mode, func(t *testing.T) {
			s, err := raft.OpenRetainedStorage(t.TempDir(), raft.RetainedIdentity{Group: "bad", Member: "a"})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.Save(raft.HardState{Term: 1}); err != nil {
				t.Fatal(err)
			}
			if err := s.SaveLog([]raft.Entry{{Index: 1, Term: 1, Noop: true}}); err != nil {
				t.Fatal(err)
			}
			data := []byte("opaque-not-an-application-checkpoint")
			if mode == "position" {
				data, err = control.NewMachine().Checkpoint(control.CheckpointPosition{Index: 2, Term: 1})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Retain(raft.RetainedSnapshot{Index: 1, Term: 1, Data: data}, 1); err != nil {
				t.Fatal(err)
			}
			node, err := raft.NewReplica(raft.Config{ID: "a", Members: []string{"a"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1}, s)
			if err != nil {
				t.Fatal(err)
			}
			if h, err := NewConfigurationWithRetention(node, map[string]string{"a": "http://127.0.0.1:1"}); err == nil || h != nil {
				t.Fatal("accepted invalid application at retained boundary")
			}
		})
	}
}
