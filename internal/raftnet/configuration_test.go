package raftnet

import (
	"bytes"
	"encoding/json"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/control"
	"github.com/Kaif10/distributed-llm-serving/internal/raft"
)

func configRequest(t *testing.T, origin, method string, update *control.Update) (int, control.Snapshot) {
	t.Helper()
	var data []byte
	if update != nil {
		data, _ = json.Marshal(update)
	}
	request, err := http.NewRequest(method, origin+"/config", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Error(err)
		return 0, control.Snapshot{}
	}
	defer response.Body.Close()
	var state control.Snapshot
	if response.StatusCode == 200 {
		if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
			t.Error(err)
		}
	}
	return response.StatusCode, state
}

func TestConfigurationCommitRetryAndConcurrentCAS(t *testing.T) {
	_, _, client := start(t, []string{"a"}, true)
	deadline := time.Now().Add(3 * time.Second)
	for {
		code, state := configRequest(t, client.URL, "GET", nil)
		if code == 200 {
			if state.Revision != 0 {
				t.Fatal(state)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no committed read")
		}
		time.Sleep(10 * time.Millisecond)
	}
	first := control.Update{RequestID: "first", ExpectedRevision: 0, Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}}
	code, state := configRequest(t, client.URL, "PUT", &first)
	if code != 200 || state.Revision != 1 {
		t.Fatalf("update: %d %+v", code, state)
	}
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, id := range []string{"second", "third"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			update := first
			update.RequestID = id
			update.ExpectedRevision = 1
			update.Workers = []control.Worker{}
			code, _ := configRequest(t, client.URL, "PUT", &update)
			codes <- code
		}()
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for code := range codes {
		counts[code]++
	}
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatal(counts)
	}
	code, state = configRequest(t, client.URL, "PUT", &first)
	if code != 200 || state.Revision != 1 || !state.Workers[0].Draining {
		t.Fatalf("retry lost original result: %d %+v", code, state)
	}
	altered := first
	altered.ExpectedRevision = 2
	code, _ = configRequest(t, client.URL, "PUT", &altered)
	if code != 409 {
		t.Fatal("reused ID accepted")
	}
	code, state = configRequest(t, client.URL, "GET", nil)
	if code != 200 || state.Revision != 2 {
		t.Fatalf("read: %d %+v", code, state)
	}
	response, err := http.Post(client.URL+"/propose", "text/plain", bytes.NewBufferString("bad"))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 404 {
		t.Fatal("raw endpoint enabled in configuration mode")
	}
	for _, body := range []string{`null`, `{"request_id":"bad","extra":true}`, `{"request_id":"bad"} {}`, `{"request_id":"bad","workers":[{"id":"a","origin":"bad"}]}`} {
		req, _ := http.NewRequest("PUT", client.URL+"/config", bytes.NewBufferString(body))
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 400 {
			t.Fatalf("invalid update accepted: %s %d", body, r.StatusCode)
		}
	}
}

func TestConfigurationRejectsRawJournal(t *testing.T) {
	store, err := raft.OpenStorage(filepath.Join(t.TempDir(), "raw.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(raft.HardState{Term: 1, VotedFor: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveLog([]raft.Entry{{Index: 1, Term: 1, Data: []byte("raw command")}}); err != nil {
		t.Fatal(err)
	}
	node, err := raft.NewReplica(raft.Config{ID: "a", Members: []string{"a"}, ElectionTicks: 2, HeartbeatTicks: 1, Seed: 1}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewConfiguration(node, map[string]string{"a": "http://127.0.0.1:1"}); err == nil {
		t.Fatal("accepted raw journal")
	}
}
