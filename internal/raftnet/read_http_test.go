package raftnet

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Kaif10/inference-fleet/internal/control"
	"github.com/Kaif10/inference-fleet/internal/raft"
)

// Establish a committed current-term no-op with an explicit protocol transcript.
// Network tests can then isolate the runtime without waiting for an unavailable
// majority to elect it. This fixture is not a live three-peer deployment.
func committedLeader(t *testing.T, members []string) (*raft.Node, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "configuration.wal")
	store, err := raft.OpenStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	node, err := raft.NewReplica(raft.Config{ID: "a", Members: members, ElectionTicks: 2, HeartbeatTicks: 1, Seed: 17}, store)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && node.Status().Role == raft.Follower; i++ {
		if _, err := node.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if len(members) > 1 {
		appends, err := node.Step(raft.Message{Type: raft.VoteResponse, From: "b", To: "a", Term: node.Status().Term, Granted: true})
		if err != nil || len(appends) == 0 {
			t.Fatal("election fixture", err)
		}
		var probe uint64
		for _, m := range appends {
			if m.To == "b" {
				probe = m.Probe
			}
		}
		if _, err := node.Step(raft.Message{Type: raft.AppendResponse, From: "b", To: "a", Term: node.Status().Term, Probe: probe, MatchIndex: 1, Success: true}); err != nil {
			t.Fatal(err)
		}
	}
	if node.Status().Role != raft.Leader || node.CommitIndex() != 1 {
		t.Fatal("fixture did not commit leader no-op")
	}
	return node, path
}

func serveConfiguration(t *testing.T, node *raft.Node, members []string) (*httptest.Server, *httptest.Server) {
	t.Helper()
	peers := map[string]string{}
	for _, id := range members {
		peers[id] = "http://127.0.0.1:1"
	}
	host, err := NewConfiguration(node, peers)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- host.Run(ctx) }()
	client, peer := httptest.NewServer(host.ClientHandler()), httptest.NewServer(host.PeerHandler())
	t.Cleanup(func() {
		cancel()
		client.Close()
		peer.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return client, peer
}
func diagnostic(t *testing.T, origin string) Snapshot {
	t.Helper()
	r, err := http.Get(origin + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var state Snapshot
	if err := json.NewDecoder(r.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	return state
}
func waitReads(t *testing.T, origin string, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if diagnostic(t, origin).PendingReads == count {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("pending reads did not reach %d", count)
}

func TestHTTPReadsDoNotGrowJournal(t *testing.T) {
	node, path := committedLeader(t, []string{"a"})
	client, _ := serveConfiguration(t, node, []string{"a"})
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1100; i++ {
		code, state := configRequest(t, client.URL, "GET", nil)
		if code != 200 || state.Revision != 0 {
			t.Fatalf("read %d: %d %+v", i, code, state)
		}
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	state := diagnostic(t, client.URL)
	if before.Size() != after.Size() || len(state.Log) != 1 || state.PendingReads != 0 {
		t.Fatalf("reads changed journal or leaked waiters: %d -> %d %+v", before.Size(), after.Size(), state)
	}
}

func TestReadWaitsForApplicationAndReplaysLegacyBarrier(t *testing.T) {
	node, _ := committedLeader(t, []string{"a"})
	update := control.Update{RequestID: "saved", Workers: []control.Worker{{ID: "worker", Origin: "http://127.0.0.1:8081", Draining: true}}}
	data, _ := json.Marshal(configurationCommand{Version: 1, Kind: "update", Update: &update})
	if _, err := node.Propose(data); err != nil {
		t.Fatal(err)
	}
	// Preserve an on-disk command emitted by the previous HTTP read implementation.
	if _, err := node.Propose([]byte(`{"version":1,"kind":"read"}`)); err != nil {
		t.Fatal(err)
	}
	host, err := NewConfiguration(node, map[string]string{"a": "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := node.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	reply := make(chan result, 1)
	pending, err := host.finishReads([]readWaiter{{ctx: context.Background(), token: token, reply: reply}})
	if err != nil || len(pending) != 1 || len(reply) != 0 {
		t.Fatal("read bypassed local application", err)
	}
	if _, err := host.applyCommitted(); err != nil {
		t.Fatal(err)
	}
	pending, err = host.finishReads(pending)
	if err != nil || len(pending) != 0 {
		t.Fatal(err)
	}
	got := <-reply
	if got.Code != 200 || got.Configuration.Revision != 1 || !got.Configuration.Workers[0].Draining || host.applied != 3 {
		t.Fatalf("bad recovered read: %+v", got)
	}
}

func TestHTTPReadCancellationReleasesContexts(t *testing.T) {
	members := []string{"a", "b", "c"}
	node, _ := committedLeader(t, members)
	client, peer := serveConfiguration(t, node, members)
	// More cancellations than the core's eight-context limit detect leaked core
	// tokens even when the runtime waiter list itself appears empty.
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, "GET", client.URL+"/config", nil)
		done := make(chan error, 1)
		go func() {
			r, err := http.DefaultClient.Do(req)
			if r != nil {
				r.Body.Close()
			}
			done <- err
		}()
		waitReads(t, client.URL, 1)
		cancel()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("isolated read returned before cancellation")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancelled client blocked")
		}
		waitReads(t, client.URL, 0)
	}
	// A live read must also be released when newer-term protocol traffic revokes
	// leadership, rather than waiting until the five-second HTTP deadline.
	done := make(chan int, 1)
	go func() {
		r, err := http.Get(client.URL + "/config")
		if err != nil {
			done <- 0
			return
		}
		r.Body.Close()
		done <- r.StatusCode
	}()
	waitReads(t, client.URL, 1)
	message := raft.Message{Type: raft.RequestVote, From: "b", To: "a", Term: diagnostic(t, client.URL).Node.Term + 1, LastIndex: 1, LastTerm: 1}
	data, _ := json.Marshal(message)
	r, err := http.Post(peer.URL+"/raft/message", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	select {
	case code := <-done:
		if code != 503 {
			t.Fatalf("read survived leadership change: %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leadership change did not release read")
	}
	waitReads(t, client.URL, 0)
}
