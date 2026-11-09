package raft

import (
	"errors"
	"fmt"
)

// CanRetainLocal requires storage capable of atomic snapshot installation.
func (n *Node) CanRetainLocal() bool {
	_, ok := n.logStorage.(*RetainedStorage)
	return n.replica && ok
}

// RetainLocal is called by the serialized application owner after constructing
// the exact committed/applied application image. No application bytes are
// interpreted here. Persist before changing in-memory indices; uncertain I/O
// fences the core. Existing read tokens remain valid in this same leader term.
func (n *Node) RetainLocal(snapshot RetainedSnapshot) error {
	if n.failed {
		return ErrStorage
	}
	if !n.CanRetainLocal() || n.role != Leader {
		return fmt.Errorf("local retention requires a retained-storage leader")
	}
	if snapshot.Index <= n.BaseIndex() || snapshot.Index != n.commit || n.applied != n.commit {
		return fmt.Errorf("local retention requires an advancing fully delivered committed prefix")
	}
	term, ok := n.TermAt(snapshot.Index)
	if !ok || term != snapshot.Term {
		return fmt.Errorf("local retention boundary term mismatch")
	}
	suffix := cloneEntries(n.entries[snapshot.Index-n.BaseIndex():])
	if err := validRetainedLog(snapshot, suffix, n.hard.Term); err != nil {
		return err
	}
	if err := n.logStorage.(*RetainedStorage).Retain(snapshot, n.applied); err != nil {
		if errors.Is(err, ErrStorage) {
			n.failed, n.role, n.leader = true, Follower, ""
		}
		return err
	}
	n.snapshot = cloneSnapshot(snapshot)
	n.entries = suffix
	n.pending = make(map[string]Message)
	n.updateTail()
	return nil
}
