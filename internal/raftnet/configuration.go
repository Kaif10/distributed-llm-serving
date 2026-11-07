package raftnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Kaif10/inference-fleet/internal/control"
	"github.com/Kaif10/inference-fleet/internal/raft"
)

// Versioned commands keep configuration logs distinct from the raw command demo.
// A read barrier is an ordinary current-term proposal. Its applied snapshot is
// returned only after the barrier commits; no lease or local leadership shortcut.
type configurationCommand struct {
	Version int             `json:"version"`
	Kind    string          `json:"kind"`
	Update  *control.Update `json:"update,omitempty"`
}

func decodeConfiguration(data []byte) (configurationCommand, error) {
	var command configurationCommand
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		return command, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return command, fmt.Errorf("expected one command")
	}
	if command.Version != 1 {
		return command, fmt.Errorf("unsupported configuration command version")
	}
	switch command.Kind {
	case "read":
		if command.Update != nil {
			return command, fmt.Errorf("read must not contain update")
		}
	case "update":
		if command.Update == nil {
			return command, fmt.Errorf("missing update")
		}
		if err := control.ValidateUpdate(*command.Update); err != nil {
			return command, err
		}
	default:
		return command, fmt.Errorf("unknown configuration command")
	}
	return command, nil
}

// NewConfiguration uses a fresh in-memory application rebuilt from the committed
// journal prefix. Do not reuse raw-proposal journals or mix peer application modes.
func NewConfiguration(node *raft.Node, peers map[string]string) (*Host, error) {
	if err := validateConfigurationEntries(node.Log()); err != nil {
		return nil, err
	}
	host := New(node, peers)
	host.machine = control.NewMachine()
	return host, nil
}

func validateConfigurationEntries(entries []raft.Entry) error {
	for _, entry := range entries {
		if !entry.Noop {
			if _, err := decodeConfiguration(entry.Data); err != nil {
				return fmt.Errorf("configuration journal index %d: %w", entry.Index, err)
			}
		}
	}
	return nil
}

// The owner invokes this after every commit-changing core operation, even when
// clients have disconnected. Results are retained only for this delivery batch;
// retry state lives in Machine and is rebuilt by replay after restart.
func (h *Host) applyCommitted() (map[uint64]result, error) {
	if h.machine == nil {
		return nil, nil
	}
	entries, err := h.node.DrainCommitted()
	if err != nil {
		return nil, err
	}
	results := make(map[uint64]result, len(entries))
	for _, entry := range entries {
		if entry.Index != h.applied+1 {
			return nil, fmt.Errorf("noncontiguous application at %d", entry.Index)
		}
		if !entry.Noop {
			command, err := decodeConfiguration(entry.Data)
			if err != nil {
				return nil, fmt.Errorf("committed configuration index %d: %w", entry.Index, err)
			}
			state := h.machine.Snapshot()
			if command.Kind == "update" {
				state, err = h.machine.Apply(*command.Update)
			}
			r := result{Configuration: &state, Code: 200}
			if err != nil {
				r.Configuration = nil
				r.Err = err.Error()
				r.Code = 400
				if errors.Is(err, control.ErrConflict) {
					r.Code = 409
				}
			}
			results[entry.Index] = r
		}
		h.applied = entry.Index
	}
	return results, nil
}

func (h *Host) configurationRoutes(mux *http.ServeMux) {
	submit := func(w http.ResponseWriter, r *http.Request, command configurationCommand) {
		data, err := json.Marshal(command)
		if err != nil || len(data) > 64<<10 {
			http.Error(w, "encoded configuration command exceeds 64 KiB", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		write(w, h.call(ctx, event{data: data}))
	}
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) {
		submit(w, r, configurationCommand{Version: 1, Kind: "read"})
	})
	mux.HandleFunc("PUT /config", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var update control.Update
		if err := decoder.Decode(&update); err != nil {
			http.Error(w, "invalid update", 400)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "expected one update", 400)
			return
		}
		if err := control.ValidateUpdate(update); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		submit(w, r, configurationCommand{Version: 1, Kind: "update", Update: &update})
	})
}
