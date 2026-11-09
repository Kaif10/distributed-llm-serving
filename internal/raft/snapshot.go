package raft

import "fmt"

const InstallSnapshot MessageType = "install_snapshot"
const SnapshotResponse MessageType = "snapshot_response"

func (n *Node) RetentionIdentity() RetainedIdentity {
	if s, ok := n.logStorage.(*RetainedStorage); ok {
		return s.Identity()
	}
	return RetainedIdentity{}
}
func (n *Node) MemberIDs() []string { return append([]string{}, n.members...) }

func (n *Node) installSnapshot(m Message) ([]Message, error) {
	if !n.CanRetainLocal() || m.Group != n.RetentionIdentity().Group || m.Snapshot == nil || m.Snapshot.Index == 0 {
		return nil, fmt.Errorf("unsupported or mismatched snapshot")
	}
	image := cloneSnapshot(*m.Snapshot)
	if err := validRetainedLog(image, nil, m.Term); err != nil {
		return nil, err
	}
	n.role, n.leader, n.votes, n.reads = Follower, m.From, nil, nil
	n.reset()
	reply := Message{Type: SnapshotResponse, From: n.id, To: m.From, Term: n.hard.Term, Probe: m.Probe, MatchIndex: image.Index, Success: true}
	if image.Index <= n.commit {
		return []Message{reply}, nil
	}
	if image.Term < n.snapshot.Term {
		return nil, fmt.Errorf("snapshot boundary term regressed")
	}
	var suffix []Entry
	if term, ok := n.TermAt(image.Index); ok && term == image.Term {
		suffix = cloneEntries(n.entries[image.Index-n.BaseIndex():])
	}
	if err := n.logStorage.(*RetainedStorage).install(image, suffix); err != nil {
		n.failed, n.role, n.leader = true, Follower, ""
		return nil, fmt.Errorf("%w: snapshot install: %v", ErrStorage, err)
	}
	n.snapshot, n.entries = image, cloneEntries(suffix)
	n.commit, n.applied = image.Index, image.Index
	n.updateTail()
	return []Message{reply}, nil
}

func (n *Node) snapshotResponse(m Message) ([]Message, error) {
	if n.role != Leader {
		return nil, nil
	}
	pending, ok := n.pending[m.From]
	if !ok || pending.Type != InstallSnapshot || pending.Snapshot == nil || pending.Probe != m.Probe {
		return nil, nil
	}
	if !m.Success || m.MatchIndex != pending.Snapshot.Index {
		return nil, nil
	}
	delete(n.pending, m.From)
	n.match[m.From] = max(n.match[m.From], m.MatchIndex)
	n.next[m.From] = n.match[m.From] + 1
	if n.advanceCommit() {
		return n.heartbeats(), nil
	}
	return n.appendMessage(m.From), nil
}

// Installation is called only after the owner validates application bytes and
// their successful history. The core supplies the compatible uncommitted suffix.
func (s *RetainedStorage) install(image RetainedSnapshot, suffix []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.image
	next.Snapshot, next.Log, next.Installed = cloneSnapshot(image), cloneEntries(suffix), true
	return s.publish(next)
}
