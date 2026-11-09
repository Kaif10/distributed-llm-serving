package raftnet

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/raft"
)

func start(t *testing.T, members []string, configuration ...bool) (*Host, *httptest.Server, *httptest.Server) {
	t.Helper()
	store, err := raft.OpenStorage(filepath.Join(t.TempDir(), "peer.wal"))
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.NewReplica(raft.Config{ID: "a", Members: members, ElectionTicks: 2, HeartbeatTicks: 1, Seed: 17}, store)
	if err != nil {
		t.Fatal(err)
	}
	peers := map[string]string{}
	for _, id := range members {
		peers[id] = "http://127.0.0.1:1"
	}
	h := New(node, peers)
	if len(configuration) > 0 && configuration[0] {
		h, err = NewConfiguration(node, peers)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	peer, client := httptest.NewServer(h.PeerHandler()), httptest.NewServer(h.ClientHandler())
	t.Cleanup(func() {
		cancel()
		peer.Close()
		client.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
		store.Close()
	})
	return h, peer, client
}

func TestSinglePeerCommitsAndSeparatesListeners(t *testing.T) {
	_, peer, client := start(t, []string{"a"})
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := http.Get(client.URL + "/status")
		if err != nil {
			t.Fatal(err)
		}
		var state Snapshot
		err = json.NewDecoder(response.Body).Decode(&state)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if state.Node.Role == raft.Leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no election")
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := http.Post(client.URL+"/propose", "application/octet-stream", bytes.NewBufferString("drain-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var ack struct {
		Committed   bool
		Index, Term uint64
	}
	json.NewDecoder(response.Body).Decode(&ack)
	if response.StatusCode != 200 || !ack.Committed || ack.Index < 2 || ack.Term == 0 {
		t.Fatalf("bad commit acknowledgement: %+v status %d", ack, response.StatusCode)
	}
	for _, url := range []string{peer.URL + "/propose", client.URL + "/raft/message"} {
		r, err := http.Post(url, "application/json", bytes.NewBufferString("{}"))
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 404 {
			t.Fatalf("listeners crossed: %d", r.StatusCode)
		}
	}
}

func TestPeerValidationAndNoQuorumDeadline(t *testing.T) {
	h, peer, client := start(t, []string{"a", "b", "c"})
	for _, body := range []string{`{"From":"unknown","To":"a"}`, `{"From":"a","To":"a"}`, `{"From":"b","To":"c"}`, `{"From":"b","To":"a","unexpected":1}`, `{} {}`} {
		r, err := http.Post(peer.URL+"/raft/message", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 400 {
			t.Fatalf("accepted invalid message %s: %d", body, r.StatusCode)
		}
	}
	r, err := http.Post(client.URL+"/propose", "application/octet-stream", bytes.NewReader(make([]byte, (64<<10)+1)))
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 400 {
		t.Fatal("oversized command accepted")
	}
	// With no reachable majority this node must never acknowledge a commit.
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	got := h.call(ctx, event{data: []byte("cannot-commit")})
	if got.Code == 200 {
		t.Fatal("minority acknowledged a proposal")
	}
	r, err = http.Get(client.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	data, _ := io.ReadAll(r.Body)
	var state Snapshot
	if err = json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	if state.Commit != 0 {
		t.Fatal("minority advanced commit")
	}
}
