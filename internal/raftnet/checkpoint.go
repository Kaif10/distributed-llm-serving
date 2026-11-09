package raftnet

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/Kaif10/distributed-llm-serving/internal/checkpoint"
	"github.com/Kaif10/distributed-llm-serving/internal/control"
	"github.com/Kaif10/distributed-llm-serving/internal/raft"
)

type CheckpointStatus struct {
	Published        *checkpoint.Selection `json:"published,omitempty"`
	RecoveryPending  bool                  `json:"recovery_pending"`
	RecoveryVerified bool                  `json:"recovery_verified"`
	RecoveryIndex    uint64                `json:"recovery_index"`
	LastError        string                `json:"last_error,omitempty"`
}
type checkpointRecovery struct {
	selection checkpoint.Selection
	machine   *control.Machine
}

// NewConfigurationWithCheckpoints leaves a selected image pending until Raft
// re-establishes its committed prefix. The caller owns Store lifetime and must
// keep it open until Run exits; only this owner may publish during Run. The
// complete Raft log remains mandatory.
func NewConfigurationWithCheckpoints(node *raft.Node, peers map[string]string, store *checkpoint.Store) (*Host, error) {
	if store == nil || store.Identity().Member != node.Status().ID {
		return nil, fmt.Errorf("checkpoint publisher must belong to local member")
	}
	host, err := NewConfiguration(node, peers)
	if err != nil {
		return nil, err
	}
	host.checkpoints = store
	host.checkpointStatus = &CheckpointStatus{}
	selected, ok, err := store.Latest()
	if err != nil {
		return nil, err
	}
	if ok {
		machine, err := host.verifyCheckpoint(selected)
		if err != nil {
			return nil, err
		}
		host.recovery = &checkpointRecovery{selection: selected, machine: machine}
		host.checkpointStatus.Published = metadata(selected)
		host.checkpointStatus.RecoveryPending = true
		host.checkpointStatus.RecoveryVerified = true
	}
	return host, nil
}
func metadata(selected checkpoint.Selection) *checkpoint.Selection {
	selected.Data = nil
	return &selected
}
func (h *Host) checkpointSnapshot() *CheckpointStatus {
	if h.checkpointStatus == nil {
		return nil
	}
	status := *h.checkpointStatus
	if status.Published != nil {
		copy := *status.Published
		status.Published = &copy
	}
	return &status
}

// With no compaction enabled, complete-prefix replay is a strong binding check:
// index/term alone cannot detect a different application history at that boundary.
// The verification machine is never exposed as applied state before commitment.
func (h *Host) verifyCheckpoint(selected checkpoint.Selection) (*control.Machine, error) {
	position := selected.Position
	log := h.node.Log()
	if position.Index > uint64(len(log)) || (position.Index > 0 && log[position.Index-1].Term != position.Term) {
		return nil, fmt.Errorf("selected checkpoint boundary does not match retained Raft log")
	}
	machine, got, err := control.RestoreCheckpoint(selected.Data)
	if err != nil {
		return nil, err
	}
	if got != position {
		return nil, fmt.Errorf("selected checkpoint position mismatch")
	}
	replay := control.NewMachine()
	for _, entry := range log[:position.Index] {
		if !entry.Noop {
			if _, err := applyConfiguration(replay, entry); err != nil {
				return nil, err
			}
		}
	}
	expected, err := replay.Checkpoint(position)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(expected, selected.Data) {
		return nil, fmt.Errorf("selected checkpoint differs from retained-prefix replay")
	}
	return machine, nil
}
func (h *Host) captureCheckpoint() (result, error) {
	if h.checkpoints == nil || h.recovery != nil || h.applied == 0 || h.applied != h.node.CommitIndex() {
		return result{Err: "checkpoint requires a fully applied committed prefix", Code: 503}, nil
	}
	log := h.node.Log()
	if h.applied > uint64(len(log)) {
		return result{}, fmt.Errorf("applied checkpoint position exceeds retained log")
	}
	position := control.CheckpointPosition{Index: h.applied, Term: log[h.applied-1].Term}
	data, err := h.machine.Checkpoint(position)
	if err != nil {
		return result{}, err
	}
	selected, err := h.checkpoints.Publish(data)
	if err != nil {
		h.checkpointStatus.LastError = err.Error()
		if errors.Is(err, checkpoint.ErrFull) {
			return result{Err: err.Error(), Code: 503}, nil
		}
		return result{}, err // Uncertain publisher I/O or invariant failure stops the owner.
	}
	h.checkpointStatus.Published = metadata(selected)
	h.checkpointStatus.LastError = ""
	return result{Checkpoint: h.checkpointSnapshot(), Code: 200}, nil
}
