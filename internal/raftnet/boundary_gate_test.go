package raftnet

import (
	"context"
	"testing"

	"github.com/Kaif10/inference-fleet/internal/raft"
)

type boundaryOnlyStorage struct{}

func (boundaryOnlyStorage) Load() (raft.HardState, error)  { return raft.HardState{Term: 1}, nil }
func (boundaryOnlyStorage) Save(raft.HardState) error      { return nil }
func (boundaryOnlyStorage) LoadLog() ([]raft.Entry, error) { return nil, nil }
func (boundaryOnlyStorage) SaveLog([]raft.Entry) error     { return nil }
func (boundaryOnlyStorage) LoadSnapshot() (raft.RetainedSnapshot, error) {
	return raft.RetainedSnapshot{Index: 20, Term: 1, Data: []byte("opaque")}, nil
}
func TestRetainedBoundaryCannotBypassRuntimeIntegrationGate(t *testing.T) {
	node, err := raft.NewReplica(raft.Config{ID: "a", Members: []string{"a"}, ElectionTicks: 5, HeartbeatTicks: 1, Seed: 1}, boundaryOnlyStorage{})
	if err != nil {
		t.Fatal(err)
	}
	peers := map[string]string{"a": "http://127.0.0.1:1"}
	if host, err := NewConfiguration(node, peers); err == nil || host != nil {
		t.Fatal("accepted boundary without application installation")
	}
	host := New(node, peers)
	if err := host.Run(context.Background()); err == nil {
		t.Fatal("raw runtime ignored retained boundary")
	}
	select {
	case <-host.done:
	default:
		t.Fatal("rejected runtime left callers waiting")
	}
}
