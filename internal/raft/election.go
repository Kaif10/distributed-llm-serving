// Package raft starts with an isolated fixed-membership election core.
// Algorithm reference: Ongaro and Ousterhout, extended Raft paper, Figure 2 / §5.2.
// New is election-only; NewReplica adds durable log replication and quorum commit.
// Network transport, distributed reads, and serving-controller integration are separate.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"unicode/utf8"
)

type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}
type Storage interface {
	Load() (HardState, error)
	Save(HardState) error
}
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type MessageType string

const (
	RequestVote  MessageType = "request_vote"
	VoteResponse MessageType = "vote_response"
	Heartbeat    MessageType = "election_heartbeat"
)

type Message struct {
	ReadID                                               uint64
	PrevIndex, PrevTerm, LeaderCommit, Probe, MatchIndex uint64
	Entries                                              []Entry
	Success                                              bool
	Type                                                 MessageType
	From, To                                             string
	Term                                                 uint64
	LastIndex, LastTerm                                  uint64
	Granted                                              bool
}
type Config struct {
	AppendBatch                   int
	ID                            string
	Members                       []string
	ElectionTicks, HeartbeatTicks int
	Seed                          int64
	LastIndex, LastTerm           uint64
}
type Status struct {
	ID               string
	Role             Role
	Term             uint64
	VotedFor, Leader string
	Failed           bool
}

var ErrStorage = errors.New("election core fenced after persistence failure")

// Node is single-threaded. Callers serialize Tick/Step, persist through Storage,
// and deliver returned messages only after the call returns successfully.
type Node struct {
	replica                                                           bool
	logStorage                                                        LogStorage
	entries                                                           []Entry
	commit, applied, probe                                            uint64
	batch                                                             int
	next, match                                                       map[string]uint64
	pending                                                           map[string]Message
	reads                                                             map[uint64]*readState
	readSequence                                                      uint64
	id                                                                string
	members                                                           []string
	member                                                            map[string]bool
	hard                                                              HardState
	role                                                              Role
	leader                                                            string
	storage                                                           Storage
	rng                                                               *rand.Rand
	electionBase, heartbeatTicks, elapsed, heartbeatElapsed, deadline int
	lastIndex, lastTerm                                               uint64
	votes                                                             map[string]bool
	failed                                                            bool
}

func New(config Config, storage Storage) (*Node, error) {
	if logs, ok := storage.(LogStorage); ok {
		entries, err := logs.LoadLog()
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			return nil, fmt.Errorf("use NewReplica for nonempty durable log")
		}
	}
	return newNode(config, storage)
}
func newNode(config Config, storage Storage) (*Node, error) {
	if storage == nil || config.ID == "" || config.ElectionTicks < 2 || config.HeartbeatTicks < 1 || config.HeartbeatTicks >= config.ElectionTicks || config.ElectionTicks > 1<<30 || config.Seed == 0 {
		return nil, fmt.Errorf("invalid election configuration")
	}
	member := make(map[string]bool)
	for _, id := range config.Members {
		if id == "" || len(id) > 64 || !utf8.ValidString(id) || member[id] {
			return nil, fmt.Errorf("invalid membership")
		}
		member[id] = true
	}
	if !member[config.ID] {
		return nil, fmt.Errorf("local ID absent from membership")
	}
	hard, err := storage.Load()
	if err != nil {
		return nil, err
	}
	if (hard.VotedFor != "" && (!member[hard.VotedFor] || hard.Term == 0)) || config.LastTerm > hard.Term || (config.LastIndex == 0) != (config.LastTerm == 0) {
		return nil, fmt.Errorf("invalid durable state or log-tail metadata")
	}
	n := &Node{id: config.ID, members: append([]string{}, config.Members...), member: member, hard: hard, role: Follower, storage: storage, rng: rand.New(rand.NewSource(config.Seed)), electionBase: config.ElectionTicks, heartbeatTicks: config.HeartbeatTicks, lastIndex: config.LastIndex, lastTerm: config.LastTerm}
	n.reset()
	return n, nil
}
func (n *Node) Status() Status {
	return Status{n.id, n.role, n.hard.Term, n.hard.VotedFor, n.leader, n.failed}
}
func (n *Node) reset() { n.elapsed = 0; n.deadline = n.electionBase + n.rng.Intn(n.electionBase) }
func (n *Node) persist(h HardState) error {
	if err := n.storage.Save(h); err != nil {
		n.failed = true
		n.role = Follower
		n.leader = ""
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	n.hard = h
	return nil
}
func (n *Node) heartbeats() []Message {
	if n.replica {
		var out []Message
		for _, id := range n.members {
			if id != n.id {
				out = append(out, n.sendAppend(id))
			}
		}
		return out
	}
	var messages []Message
	for _, id := range n.members {
		if id != n.id {
			messages = append(messages, Message{Type: Heartbeat, From: n.id, To: id, Term: n.hard.Term})
		}
	}
	return messages
}
func (n *Node) elected() ([]Message, error) {
	n.reads = nil
	n.role = Leader
	n.leader = n.id
	n.heartbeatElapsed = 0
	if n.replica {
		n.next = make(map[string]uint64)
		n.match = make(map[string]uint64)
		n.pending = make(map[string]Message)
		for _, id := range n.members {
			n.next[id] = n.lastIndex + 1
		}
		entries := append(cloneEntries(n.entries), Entry{Index: n.lastIndex + 1, Term: n.hard.Term, Noop: true})
		if err := n.saveLog(entries); err != nil {
			n.failed = true
			n.role = Follower
			n.leader = ""
			return nil, err
		}
		n.match[n.id] = n.lastIndex
		n.advanceCommit()
	}
	return n.heartbeats(), nil
}
func (n *Node) campaign() ([]Message, error) {
	n.reads = nil
	if n.hard.Term == ^uint64(0) {
		n.failed = true
		n.role = Follower
		n.leader = ""
		return nil, fmt.Errorf("%w: term exhausted", ErrStorage)
	}
	if err := n.persist(HardState{Term: n.hard.Term + 1, VotedFor: n.id}); err != nil {
		return nil, err
	}
	n.role = Candidate
	n.leader = ""
	n.reset()
	n.votes = map[string]bool{n.id: true}
	if len(n.votes) > len(n.members)/2 {
		return n.elected()
	}
	var messages []Message
	for _, id := range n.members {
		if id != n.id {
			messages = append(messages, Message{Type: RequestVote, From: n.id, To: id, Term: n.hard.Term, LastIndex: n.lastIndex, LastTerm: n.lastTerm})
		}
	}
	return messages, nil
}
func (n *Node) Tick() ([]Message, error) {
	if n.failed {
		return nil, ErrStorage
	}
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTicks {
			n.heartbeatElapsed = 0
			return append(n.heartbeats(), n.readMessages()...), nil
		}
		return nil, nil
	}
	n.elapsed++
	if n.elapsed >= n.deadline {
		return n.campaign()
	}
	return nil, nil
}
func (n *Node) Step(m Message) ([]Message, error) {
	if n.failed {
		return nil, ErrStorage
	}
	if m.Term == 0 || m.To != n.id || m.From == n.id || !n.member[m.From] || (m.Type != RequestVote && m.Type != VoteResponse && m.Type != Heartbeat && m.Type != AppendEntries && m.Type != AppendResponse && m.Type != ReadProbe && m.Type != ReadResponse) {
		return nil, nil
	}
	if m.Term > n.hard.Term {
		if err := n.persist(HardState{Term: m.Term}); err != nil {
			return nil, err
		}
		n.role = Follower
		n.leader = ""
		n.votes = nil
		n.reads = nil
	}
	if n.replica && m.Type == Heartbeat {
		return nil, nil
	}
	if !n.replica && (m.Type == AppendEntries || m.Type == AppendResponse || m.Type == ReadProbe || m.Type == ReadResponse) {
		return nil, nil
	}
	if m.Type == RequestVote {
		fresh := m.LastTerm <= m.Term && ((m.LastIndex == 0) == (m.LastTerm == 0)) && (m.LastTerm > n.lastTerm || (m.LastTerm == n.lastTerm && m.LastIndex >= n.lastIndex))
		grant := m.Term == n.hard.Term && m.Term > 0 && fresh && (n.hard.VotedFor == "" || n.hard.VotedFor == m.From)
		if grant {
			if n.hard.VotedFor != m.From {
				if err := n.persist(HardState{Term: n.hard.Term, VotedFor: m.From}); err != nil {
					return nil, err
				}
			}
			n.reset()
		}
		return []Message{{Type: VoteResponse, From: n.id, To: m.From, Term: n.hard.Term, Granted: grant}}, nil
	}
	if m.Term != n.hard.Term {
		if n.replica && m.Type == ReadProbe {
			return []Message{{Type: ReadResponse, From: n.id, To: m.From, Term: n.hard.Term, ReadID: m.ReadID}}, nil
		}
		if n.replica && m.Type == AppendEntries {
			return []Message{{Type: AppendResponse, From: n.id, To: m.From, Term: n.hard.Term, Probe: m.Probe}}, nil
		}
		return nil, nil
	}
	if m.Type == AppendEntries {
		return n.appendRequest(m)
	}
	if m.Type == ReadProbe {
		return n.confirmRead(m), nil
	}
	if m.Type == ReadResponse {
		n.receiveRead(m)
		return nil, nil
	}
	if m.Type == AppendResponse {
		return n.appendResponse(m)
	}
	if m.Type == Heartbeat {
		n.role = Follower
		n.leader = m.From
		n.votes = nil
		n.reset()
		return nil, nil
	}
	if n.role == Candidate && m.Granted {
		n.votes[m.From] = true
		if len(n.votes) > len(n.members)/2 {
			return n.elected()
		}
	}
	return nil, nil
}
