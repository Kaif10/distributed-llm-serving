package raft

import (
	"errors"
	"reflect"
	"testing"
)

func readCluster(t *testing.T, ids []string) map[string]*Node {
	t.Helper()
	nodes := map[string]*Node{}
	for i, id := range ids {
		nodes[id] = replica(t, id, ids, &logMemory{}, int64(i+1), 16)
	}
	deliver(t, nodes, campaign(t, nodes[ids[0]]))
	return nodes
}

func TestReadRequiresFreshDistinctMajority(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	nodes := readCluster(t, ids)
	leader := nodes["a"]
	before := leader.Log()
	token, requests, err := leader.BeginRead()
	if err != nil || token.Index != leader.commit {
		t.Fatal(token, err)
	}
	ack, err := nodes["b"].Step(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	leader.Step(ack[0])
	leader.Step(ack[0])
	invalid := ack[0]
	invalid.From = "outsider"
	leader.Step(invalid)
	invalid = ack[0]
	invalid.From = "c"
	invalid.ReadID++
	leader.Step(invalid)
	invalid = ack[0]
	invalid.From = "c"
	invalid.Term--
	leader.Step(invalid)
	if ready, err := leader.ReadReady(token); err != nil || ready {
		t.Fatalf("duplicate/stale/unknown reply formed quorum: %v %v", ready, err)
	}
	tick, err := leader.Tick()
	if err != nil {
		t.Fatal(err)
	}
	retransmits := 0
	for _, m := range tick {
		if m.Type == ReadProbe {
			if m.ReadID != token.ID || m.To == "b" {
				t.Fatal("wrong read retransmission", m)
			}
			retransmits++
		}
	}
	if retransmits != 3 {
		t.Fatalf("missing read retransmissions: %d", retransmits)
	}
	cAck, err := nodes["c"].Step(requests[1])
	if err != nil {
		t.Fatal(err)
	}
	leader.Step(cAck[0])
	if ready, err := leader.ReadReady(token); err != nil || !ready {
		t.Fatal("majority failed", ready, err)
	}
	leader.ForgetRead(token)
	next, _, err := leader.BeginRead()
	if err != nil || next.ID == token.ID {
		t.Fatal("context reused", err)
	}
	leader.Step(ack[0])
	leader.Step(cAck[0])
	if ready, err := leader.ReadReady(next); err != nil || ready {
		t.Fatal("old confirmations satisfied later read", ready, err)
	}
	if !reflect.DeepEqual(before, leader.Log()) {
		t.Fatal("read appended to log")
	}
	leader.ForgetRead(next)
	if _, err := leader.ReadReady(next); err == nil {
		t.Fatal("forgotten context remained usable")
	}
}

func TestReadWaitsForCurrentTermCommit(t *testing.T) {
	ids := []string{"a", "b", "c"}
	nodes := map[string]*Node{}
	for i, id := range ids {
		nodes[id] = replica(t, id, ids, &logMemory{memory: memory{state: HardState{Term: 1}}, entries: []Entry{{Index: 1, Term: 1, Noop: true}}}, int64(i+1), 16)
		nodes[id].commit = 1 // A previously committed prefix, retained across a later election.
	}
	leader := nodes["a"]
	votes := campaign(t, leader)
	reply, err := nodes["b"].Step(votes[0])
	if err != nil {
		t.Fatal(err)
	}
	appends, err := leader.Step(reply[0])
	if err != nil || leader.role != Leader {
		t.Fatal("no leader", err)
	}
	if _, _, err := leader.BeginRead(); !errors.Is(err, ErrReadNotReady) {
		t.Fatal("read accepted only prior-term commit", err)
	}
	deliver(t, nodes, appends)
	token, requests, err := leader.BeginRead()
	if err != nil || token.Index != 2 {
		t.Fatal(token, err)
	}
	deliver(t, nodes, requests)
	if ready, err := leader.ReadReady(token); err != nil || !ready {
		t.Fatal(ready, err)
	}
}

func TestReadTermChangeAndRestartFenceReplies(t *testing.T) {
	ids := []string{"a", "b", "c"}
	nodes := readCluster(t, ids)
	old := nodes["a"]
	token, requests, err := old.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	delayed, err := nodes["b"].Step(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	// A current-term reply from a different context is harmless, whereas any
	// higher-term member response must invalidate leadership before completion.
	_, err = old.Step(Message{Type: ReadResponse, From: "c", To: "a", Term: token.Term + 1, ReadID: token.ID, Success: true})
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := old.ReadReady(token); ready || !errors.Is(err, ErrNotLeader) {
		t.Fatal("old term read survived", ready, err)
	}
	restarted := replica(t, "a", ids, old.logStorage, 19, 16)
	nodes["a"] = restarted
	deliver(t, nodes, campaign(t, restarted))
	fresh, _, err := restarted.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Term == token.Term {
		t.Fatal("restart reused leader term")
	}
	restarted.Step(delayed[0])
	if ready, err := restarted.ReadReady(fresh); err != nil || ready {
		t.Fatal("pre-restart reply satisfied read", ready, err)
	}
}

func TestReadBoundsAndNoAcknowledgementAfterPersistenceFailure(t *testing.T) {
	nodes := readCluster(t, []string{"a"})
	leader := nodes["a"]
	var tokens []ReadToken
	for i := 0; i < 8; i++ {
		token, messages, err := leader.BeginRead()
		if err != nil || len(messages) != 0 {
			t.Fatal(err)
		}
		if ready, err := leader.ReadReady(token); !ready || err != nil {
			t.Fatal(ready, err)
		}
		tokens = append(tokens, token)
	}
	if _, _, err := leader.BeginRead(); err == nil {
		t.Fatal("unbounded read contexts")
	}
	leader.ForgetRead(tokens[0])
	if _, _, err := leader.BeginRead(); err != nil {
		t.Fatal("context was not released", err)
	}
	follower := replica(t, "b", []string{"a", "b", "c"}, &logMemory{memory: memory{fail: true}}, 2, 16)
	out, err := follower.Step(Message{Type: ReadProbe, From: "a", To: "b", Term: 3, ReadID: 1})
	if !errors.Is(err, ErrStorage) || len(out) != 0 {
		t.Fatal("confirmed before durable term change", out, err)
	}
	if _, _, err := follower.BeginRead(); !errors.Is(err, ErrStorage) {
		t.Fatal("fenced node started a read", err)
	}
}
