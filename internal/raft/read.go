package raft

import (
	"errors"
	"fmt"
	"sort"
)

const (
	ReadProbe    MessageType = "read_probe"
	ReadResponse MessageType = "read_response"
)

var ErrReadNotReady = errors.New("current-term entry not yet committed")

// ReadToken identifies a fresh read within one leader term. Index is the
// committed prefix captured after request arrival; it must be locally applied.
type ReadToken struct{ ID, Term, Index uint64 }
type readState struct {
	token ReadToken
	acks  map[string]bool
}

// BeginRead does not acknowledge a read or append a log entry. It starts a
// distinct majority confirmation, allowed only after a current-term commit.
// Call ReadReady, wait for local application, then ForgetRead on every exit.
func (n *Node) BeginRead() (ReadToken, []Message, error) {
	if n.failed {
		return ReadToken{}, nil, ErrStorage
	}
	if !n.replica || n.role != Leader {
		return ReadToken{}, nil, ErrNotLeader
	}
	if n.commit == 0 || n.entries[n.commit-1].Term != n.hard.Term {
		return ReadToken{}, nil, ErrReadNotReady
	}
	if len(n.reads) >= 8 {
		return ReadToken{}, nil, fmt.Errorf("read contexts full")
	}
	if n.readSequence == ^uint64(0) {
		return ReadToken{}, nil, fmt.Errorf("read context IDs exhausted")
	}
	n.readSequence++
	token := ReadToken{ID: n.readSequence, Term: n.hard.Term, Index: n.commit}
	if n.reads == nil {
		n.reads = make(map[uint64]*readState)
	}
	state := &readState{token: token, acks: map[string]bool{n.id: true}}
	n.reads[token.ID] = state
	return token, n.messagesForRead(state), nil
}

func (n *Node) ReadReady(token ReadToken) (bool, error) {
	if n.failed {
		return false, ErrStorage
	}
	if n.role != Leader || n.hard.Term != token.Term {
		return false, ErrNotLeader
	}
	state, ok := n.reads[token.ID]
	if !ok || state.token != token {
		return false, fmt.Errorf("unknown read context")
	}
	return len(state.acks) > len(n.members)/2, nil
}
func (n *Node) ForgetRead(token ReadToken) {
	if state := n.reads[token.ID]; state != nil && state.token == token {
		delete(n.reads, token.ID)
	}
}

func (n *Node) messagesForRead(state *readState) []Message {
	if len(state.acks) > len(n.members)/2 {
		return nil
	}
	var out []Message
	for _, peer := range n.members {
		if !state.acks[peer] {
			out = append(out, Message{Type: ReadProbe, From: n.id, To: peer, Term: state.token.Term, ReadID: state.token.ID})
		}
	}
	return out
}
func (n *Node) readMessages() []Message {
	// Stable order lets completed/expired early contexts release sender capacity
	// for later contexts under bounded queue drops.
	ids := make([]uint64, 0, len(n.reads))
	for id := range n.reads {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []Message
	for _, id := range ids {
		out = append(out, n.messagesForRead(n.reads[id])...)
	}
	return out
}
func (n *Node) confirmRead(m Message) []Message {
	if m.ReadID == 0 {
		return nil
	}
	n.role = Follower
	n.leader = m.From
	n.votes = nil
	n.reads = nil
	n.reset()
	return []Message{{Type: ReadResponse, From: n.id, To: m.From, Term: n.hard.Term, ReadID: m.ReadID, Success: true}}
}
func (n *Node) receiveRead(m Message) {
	if n.role != Leader || !m.Success {
		return
	}
	state := n.reads[m.ReadID]
	if state == nil || state.token.Term != n.hard.Term {
		return
	}
	state.acks[m.From] = true
}
