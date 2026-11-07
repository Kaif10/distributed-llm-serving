package raft

import (
	"bytes"
	"errors"
	"fmt"
)

type Entry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data,omitempty"`
	Noop  bool   `json:"noop,omitempty"`
}
type LogStorage interface {
	Storage
	LoadLog() ([]Entry, error)
	SaveLog([]Entry) error
}

const (
	AppendEntries  MessageType = "append_entries"
	AppendResponse MessageType = "append_response"
)

var ErrNotLeader = errors.New("not a replication leader")

func cloneEntries(entries []Entry) []Entry {
	out := append([]Entry{}, entries...)
	for i := range out {
		out[i].Data = append([]byte(nil), out[i].Data...)
	}
	return out
}
func validLog(entries []Entry, term uint64) error {
	if len(entries) > 1024 {
		return fmt.Errorf("log exceeds 1024 entries")
	}
	size := 0
	var prior uint64
	for i, e := range entries {
		if e.Index != uint64(i+1) || e.Term == 0 || e.Term > term || e.Term < prior || len(e.Data) > 64<<10 || (e.Noop && len(e.Data) != 0) || (!e.Noop && len(e.Data) == 0) {
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

// NewReplica enables replication using a durable log, unlike election-only New.
func NewReplica(config Config, storage LogStorage) (*Node, error) {
	n, err := newNode(config, storage)
	if err != nil {
		return nil, err
	}
	entries, err := storage.LoadLog()
	if err != nil {
		return nil, err
	}
	if err := validLog(entries, n.hard.Term); err != nil {
		return nil, err
	}
	if config.LastIndex != 0 || config.LastTerm != 0 {
		return nil, fmt.Errorf("replication derives log-tail metadata from storage")
	}
	n.replica = true
	n.logStorage = storage
	n.entries = cloneEntries(entries)
	n.updateTail()
	n.batch = config.AppendBatch
	if n.batch == 0 {
		n.batch = 16
	}
	if n.batch < 1 || n.batch > 64 {
		return nil, fmt.Errorf("append batch must be 1..64")
	}
	return n, nil
}
func (n *Node) updateTail() {
	n.lastIndex = uint64(len(n.entries))
	n.lastTerm = 0
	if len(n.entries) > 0 {
		n.lastTerm = n.entries[len(n.entries)-1].Term
	}
}
func (n *Node) saveLog(entries []Entry) error {
	if err := validLog(entries, n.hard.Term); err != nil {
		return err
	}
	if err := n.logStorage.SaveLog(entries); err != nil {
		n.failed = true
		n.role = Follower
		n.leader = ""
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	n.entries = cloneEntries(entries)
	n.updateTail()
	return nil
}
func (n *Node) Propose(data []byte) ([]Message, error) {
	if n.failed {
		return nil, ErrStorage
	}
	if !n.replica || n.role != Leader {
		return nil, ErrNotLeader
	}
	if len(data) == 0 || len(data) > 64<<10 {
		return nil, fmt.Errorf("command must contain 1..65536 bytes")
	}
	entries := append(cloneEntries(n.entries), Entry{Index: n.lastIndex + 1, Term: n.hard.Term, Data: append([]byte(nil), data...)})
	if err := n.saveLog(entries); err != nil {
		return nil, err
	}
	n.match[n.id] = n.lastIndex
	n.advanceCommit()
	return n.heartbeats(), nil
}
func (n *Node) CommitIndex() uint64 { return n.commit }

// DrainCommitted is a volatile delivery cursor, not a durable application checkpoint.
func (n *Node) DrainCommitted() ([]Entry, error) {
	if n.failed {
		return nil, ErrStorage
	}
	out := cloneEntries(n.entries[n.applied:n.commit])
	n.applied = n.commit
	return out, nil
}
func (n *Node) Log() []Entry { return cloneEntries(n.entries) }
func (n *Node) sendAppend(peer string) Message {
	if pending, ok := n.pending[peer]; ok {
		pending.Entries = cloneEntries(pending.Entries)
		return pending
	}
	n.probe++
	next := n.next[peer]
	end := min(n.lastIndex+1, next+uint64(n.batch))
	prev := next - 1
	var prevTerm uint64
	if prev > 0 {
		prevTerm = n.entries[prev-1].Term
	}
	m := Message{Type: AppendEntries, From: n.id, To: peer, Term: n.hard.Term, PrevIndex: prev, PrevTerm: prevTerm, Entries: cloneEntries(n.entries[next-1 : end-1]), LeaderCommit: n.commit, Probe: n.probe}
	n.pending[peer] = m
	m.Entries = cloneEntries(m.Entries)
	return m
}
func (n *Node) advanceCommit() bool {
	for index := n.lastIndex; index > n.commit; index-- {
		if n.entries[index-1].Term != n.hard.Term {
			continue
		}
		votes := 0
		for _, id := range n.members {
			if n.match[id] >= index {
				votes++
			}
		}
		if votes > len(n.members)/2 {
			n.commit = index
			return true
		}
	}
	return false
}
func (n *Node) appendRequest(m Message) ([]Message, error) {
	if len(m.Entries) > 64 || (m.PrevIndex == 0) != (m.PrevTerm == 0) || m.PrevTerm > m.Term || m.PrevIndex > 1024 {
		return nil, fmt.Errorf("invalid append prefix or batch")
	}
	prior := m.PrevTerm
	for i, e := range m.Entries {
		if e.Index != m.PrevIndex+uint64(i)+1 || e.Term < prior || e.Term > m.Term || e.Term == 0 || len(e.Data) > 64<<10 || (e.Noop && len(e.Data) != 0) || (!e.Noop && len(e.Data) == 0) {
			return nil, fmt.Errorf("invalid append entry")
		}
		prior = e.Term
	}
	n.role = Follower
	n.leader = m.From
	n.votes = nil
	n.reads = nil
	n.reset()
	reply := Message{Type: AppendResponse, From: n.id, To: m.From, Term: n.hard.Term, Probe: m.Probe}
	if m.PrevIndex > n.lastIndex || (m.PrevIndex > 0 && n.entries[m.PrevIndex-1].Term != m.PrevTerm) {
		return []Message{reply}, nil
	}
	entries := cloneEntries(n.entries)
	changed := false
	for i, e := range m.Entries {
		if e.Index <= uint64(len(entries)) {
			old := entries[e.Index-1]
			if old.Term == e.Term {
				if old.Noop != e.Noop || !bytes.Equal(old.Data, e.Data) {
					return nil, fmt.Errorf("same index/term has different command")
				}
				continue
			}
			if e.Index <= n.commit {
				return nil, fmt.Errorf("attempted replacement of committed entry")
			}
			entries = entries[:e.Index-1]
		}
		entries = append(entries, cloneEntries(m.Entries[i:])...)
		changed = true
		break
	}
	if changed {
		if err := n.saveLog(entries); err != nil {
			return nil, err
		}
	}
	matched := m.PrevIndex + uint64(len(m.Entries))
	if m.LeaderCommit > n.commit {
		n.commit = max(n.commit, min(m.LeaderCommit, matched))
	}
	reply.Success = true
	reply.MatchIndex = matched
	return []Message{reply}, nil
}
func (n *Node) appendResponse(m Message) ([]Message, error) {
	if n.role != Leader {
		return nil, nil
	}
	pending, ok := n.pending[m.From]
	if !ok || m.Probe != pending.Probe {
		return nil, nil
	}
	last := pending.PrevIndex + uint64(len(pending.Entries))
	if m.Success && m.MatchIndex != last {
		return nil, nil
	}
	delete(n.pending, m.From)
	if !m.Success {
		n.next[m.From] = max(n.match[m.From]+1, max(uint64(1), pending.PrevIndex))
		return []Message{n.sendAppend(m.From)}, nil
	}
	n.match[m.From] = max(n.match[m.From], last)
	n.next[m.From] = n.match[m.From] + 1
	if n.advanceCommit() {
		return n.heartbeats(), nil
	}
	if n.next[m.From] <= n.lastIndex || pending.LeaderCommit < n.commit {
		return []Message{n.sendAppend(m.From)}, nil
	}
	return nil, nil
}
