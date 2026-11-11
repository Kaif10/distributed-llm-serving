package raftnet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/control"
	"github.com/Kaif10/distributed-llm-serving/internal/raft"
)

// A real legacy-command prefix seeds the boundary without issuing 1023 HTTP
// requests. The image contains exactly those ordered successful updates.
func nearCapacityStore(t *testing.T, dir string) *raft.RetainedStorage {
	t.Helper()
	s, err := raft.OpenRetainedStorage(dir, raft.RetainedIdentity{Group: "admission", Member: "a"})
	if err != nil {
		t.Fatal(err)
	}
	m := control.NewMachine()
	entries := []raft.Entry{{Index: 1, Term: 1, Noop: true}}
	for i := 0; i < 1023; i++ {
		u := control.Update{RequestID: fmt.Sprint(i), ExpectedRevision: uint64(i), Workers: []control.Worker{}}
		if _, err := m.Apply(u); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(configurationCommand{Version: 1, Kind: "update", Update: &u})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, raft.Entry{Index: uint64(i + 2), Term: 1, Data: data})
	}
	if err := s.Save(raft.HardState{Term: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveLog(entries); err != nil {
		t.Fatal(err)
	}
	data, err := m.Checkpoint(control.CheckpointPosition{Index: 1024, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Retain(raft.RetainedSnapshot{Index: 1024, Term: 1, Data: data}, 1024); err != nil {
		t.Fatal(err)
	}
	return s
}

func servingRetainedHost(t *testing.T, s *raft.RetainedStorage) (*httptest.Server, func()) {
	t.Helper()
	h := retainedHost(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	server := httptest.NewServer(h.ClientHandler())
	stop := func() {
		cancel()
		server.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		code, state := configRequest(t, server.URL, "GET", nil)
		if code == 200 {
			if state.Revision < 1023 {
				t.Fatal(state)
			}
			return server, stop
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatal("no fresh read after retained restart")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func admissionStatus(t *testing.T, origin string) Snapshot {
	t.Helper()
	response, err := http.Get(origin + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var state Snapshot
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestRetainedAdmissionAtCapacityAcrossNativeFileRestart(t *testing.T) {
	dir := t.TempDir()
	s := nearCapacityStore(t, dir)
	server, stop := servingRetainedHost(t, s)
	// Always join the owner before closing its store, including assertion failures.
	defer func() {
		if stop != nil {
			stop()
		}
		s.Close()
	}()
	last := control.Update{RequestID: "last", ExpectedRevision: 1023, Workers: []control.Worker{}}
	if code, state := configRequest(t, server.URL, "PUT", &last); code != 200 || state.Revision != 1024 {
		t.Fatal(code, state)
	}
	before := admissionStatus(t, server.URL)
	var command configurationCommand
	if err := json.Unmarshal(before.Log[len(before.Log)-1].Data, &command); err != nil || command.Version != 2 {
		t.Fatal("new HTTP command not versioned", command, err)
	}
	denied := control.Update{RequestID: "too-many", ExpectedRevision: 1024, Workers: []control.Worker{}}
	for i := 0; i < 3; i++ {
		if code, _ := configRequest(t, server.URL, "PUT", &denied); code != 503 {
			t.Fatal("capacity was acknowledged", code)
		}
	}
	if after := admissionStatus(t, server.URL); after.Commit != before.Commit || after.Configuration.Revision != 1024 {
		t.Fatal("known-full rejection grew log/state", after.Commit, before.Commit)
	}
	response, err := http.Post(server.URL+"/retain", "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("last acknowledged state cannot be retained", response.StatusCode)
	}
	first := control.Update{RequestID: "0", Workers: []control.Worker{}}
	if code, state := configRequest(t, server.URL, "PUT", &first); code != 200 || state.Revision != 1 {
		t.Fatal("historical retry rejected at capacity", code, state)
	}
	stop()
	stop = nil
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = raft.OpenRetainedStorage(dir, raft.RetainedIdentity{Group: "admission", Member: "a"})
	if err != nil {
		t.Fatal(err)
	}
	server, stop = servingRetainedHost(t, s)
	if code, state := configRequest(t, server.URL, "GET", nil); code != 200 || state.Revision != 1024 {
		t.Fatal("restart changed state", code, state)
	}
	if code, state := configRequest(t, server.URL, "PUT", &first); code != 200 || state.Revision != 1 {
		t.Fatal("restart lost original result", code, state)
	}
	if code, _ := configRequest(t, server.URL, "PUT", &denied); code != 503 {
		t.Fatal("restart forgot capacity policy", code)
	}
}

func TestOrderedBoundedCommandRejectsCapacityEvenWithoutPreflight(t *testing.T) {
	m := control.NewMachine()
	for i := 0; i < 1024; i++ {
		if _, err := m.Apply(control.Update{RequestID: fmt.Sprint(i), ExpectedRevision: uint64(i), Workers: []control.Worker{}}); err != nil {
			t.Fatal(err)
		}
	}
	u := control.Update{RequestID: "new", ExpectedRevision: 1024, Workers: []control.Worker{}}
	data, _ := json.Marshal(configurationCommand{Version: 2, Kind: "update", Update: &u})
	r, err := applyConfiguration(m, raft.Entry{Index: 1025, Term: 2, Data: data})
	if err != nil || r.Code != 503 || m.Snapshot().Revision != 1024 {
		t.Fatal("ordered apply bypassed capacity", r, err)
	}
	legacy, _ := json.Marshal(configurationCommand{Version: 1, Kind: "update", Update: &u})
	r, err = applyConfiguration(m, raft.Entry{Index: 1026, Term: 2, Data: legacy})
	if err != nil || r.Code != 200 || m.Snapshot().Revision != 1025 {
		t.Fatal("legacy log replay semantics changed", r, err)
	}
}
