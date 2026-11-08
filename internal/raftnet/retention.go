package raftnet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Kaif10/inference-fleet/internal/control"
	"github.com/Kaif10/inference-fleet/internal/raft"
)

type RetentionStatus struct {
	BaseIndex      uint64 `json:"base_index"`
	BaseTerm       uint64 `json:"base_term"`
	RecoveredIndex uint64 `json:"recovered_index"`
	SHA256         string `json:"sha256,omitempty"`
	Bytes          int    `json:"checkpoint_bytes"`
}

// NewConfigurationWithRetention restores a trusted local retained image before
// applying the suffix. Only single-member groups are supported. Unlike the
// independent publication journal, this store's boundary certifies commitment.
func NewConfigurationWithRetention(node *raft.Node, peers map[string]string) (*Host, error) {
	if !node.CanRetainLocal() || len(peers) != 1 || peers[node.Status().ID] == "" {
		return nil, fmt.Errorf("retention requires a single-member configuration and retained storage")
	}
	if err := validateConfigurationEntries(node.Log()); err != nil {
		return nil, err
	}
	h := New(node, peers)
	h.machine = control.NewMachine()
	h.retention = &RetentionStatus{}
	image := node.Snapshot()
	if image.Index != 0 {
		machine, position, err := control.RestoreCheckpoint(image.Data)
		if err != nil {
			return nil, err
		}
		if position.Index != image.Index || position.Term != image.Term {
			return nil, fmt.Errorf("retained application position differs from durable boundary")
		}
		h.machine = machine
		h.applied = image.Index
		h.retention = retentionMetadata(image, image.Index)
	}
	return h, nil
}

func retentionMetadata(image raft.RetainedSnapshot, recovered uint64) *RetentionStatus {
	digest := sha256.Sum256(image.Data)
	return &RetentionStatus{BaseIndex: image.Index, BaseTerm: image.Term, RecoveredIndex: recovered, SHA256: hex.EncodeToString(digest[:]), Bytes: len(image.Data)}
}
func (h *Host) retentionSnapshot() *RetentionStatus {
	if h.retention == nil {
		return nil
	}
	copy := *h.retention
	return &copy
}
func (h *Host) retainLocal() (result, error) {
	if h.retention == nil || h.machine == nil || h.node.Status().Role != raft.Leader || h.applied == 0 || h.applied != h.node.CommitIndex() {
		return result{Err: "retention requires a fully applied single-member leader", Code: 503}, nil
	}
	if h.applied == h.node.BaseIndex() {
		return result{Retention: h.retentionSnapshot(), Code: 200}, nil
	}
	term, ok := h.node.TermAt(h.applied)
	if !ok {
		return result{}, fmt.Errorf("applied retention term missing")
	}
	data, err := h.machine.Checkpoint(control.CheckpointPosition{Index: h.applied, Term: term})
	if err != nil {
		// Codec/history bounds reject this maintenance operation, not reads or
		// already acknowledged state. No request-ID history is evicted.
		return result{Err: err.Error(), Code: 503}, nil
	}
	image := raft.RetainedSnapshot{Index: h.applied, Term: term, Data: data}
	if err := h.node.RetainLocal(image); err != nil {
		return result{}, err // Owner stops on uncertain storage or invariant failure.
	}
	h.retention = retentionMetadata(image, h.retention.RecoveredIndex)
	return result{Retention: h.retentionSnapshot(), Code: 200}, nil
}
func (h *Host) retentionRoute(mux *http.ServeMux) {
	if h.retention == nil {
		return
	}
	mux.HandleFunc("POST /retain", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1)
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) != 0 {
			http.Error(w, "retention requires an empty body", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		write(w, h.call(ctx, event{retain: true}))
	})
}
