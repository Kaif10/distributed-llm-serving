package raft

import (
	"fmt"
	"sort"
)

// RetainedSnapshot is an opaque application image at a durable committed prefix.
// A storage adapter must recover this and its suffix as one coherent selection.
// The core does not authenticate or interpret application bytes.
type RetainedSnapshot struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data,omitempty"`
}

// SnapshotSource must expose hard state, boundary, and suffix from one coherent
// durable selection, with exclusive ownership while NewReplica reads them.
// A nonzero boundary certifies commitment; an independently captured application
// checkpoint alone does not satisfy this contract.
type SnapshotSource interface {
	LogStorage
	LoadSnapshot() (RetainedSnapshot, error)
}

// BaseIndex is the absolute index included by the durable retained snapshot.
func (n *Node) BaseIndex() uint64 { return n.snapshot.Index }

func cloneSnapshot(snapshot RetainedSnapshot) RetainedSnapshot {
	snapshot.Data = append([]byte(nil), snapshot.Data...)
	return snapshot
}
func validRetainedLog(snapshot RetainedSnapshot, entries []Entry, term uint64) error {
	if (snapshot.Index == 0) != (snapshot.Term == 0) || snapshot.Term > term || snapshot.Index == ^uint64(0) || len(snapshot.Data) > (8<<20)+12 || (snapshot.Index == 0 && len(snapshot.Data) != 0) || (snapshot.Index > 0 && len(snapshot.Data) == 0) {
		return fmt.Errorf("invalid retained snapshot boundary")
	}
	if len(entries) > 1024 {
		return fmt.Errorf("log exceeds 1024 entries")
	}
	// Keep one representable next-index sentinel; all index arithmetic stays exact.
	if uint64(len(entries)) > ^uint64(0)-1-snapshot.Index {
		return fmt.Errorf("log index exhausted")
	}
	size := 0
	prior := snapshot.Term
	for i, e := range entries {
		if e.Index != snapshot.Index+uint64(i)+1 || e.Term == 0 || e.Term > term || e.Term < prior || len(e.Data) > 64<<10 || (e.Noop && len(e.Data) != 0) || (!e.Noop && len(e.Data) == 0) {
			return fmt.Errorf("invalid log entry")
		}
		prior = e.Term
		size += len(e.Data)
	}
	if size > 4<<20 {
		return fmt.Errorf("log exceeds four MiB")
	}
	return nil
}

// Snapshot returns independent bytes. A future adapter must restore the snapshot
// before applying DrainCommitted's suffix; the durable boundary is already committed.
func (n *Node) Snapshot() RetainedSnapshot { return cloneSnapshot(n.snapshot) }
func (n *Node) TermAt(index uint64) (uint64, bool) {
	if index == n.snapshot.Index {
		return n.snapshot.Term, true
	}
	if index < n.snapshot.Index || index > n.lastIndex {
		return 0, false
	}
	return n.entries[index-n.snapshot.Index-1].Term, true
}

// PendingSnapshotPeers identifies leader peers whose requested prefix was
// removed. Until snapshot transfer exists, no invalid AppendEntries is emitted
// for them and they cannot contribute new replication acknowledgements.
func (n *Node) PendingSnapshotPeers() []string {
	var peers []string
	if n.role != Leader {
		return peers
	}
	for _, id := range n.members {
		if id != n.id && n.next[id] <= n.snapshot.Index {
			peers = append(peers, id)
		}
	}
	sort.Strings(peers)
	return peers
}
func (n *Node) appendMessage(peer string) []Message {
	m := n.sendAppend(peer)
	if m.To == "" {
		return nil
	}
	return []Message{m}
}
