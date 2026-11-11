package control

import (
	"errors"
	"fmt"
)

var ErrCapacity = errors.New("retained application history capacity reached")

// CheckCheckpointAdmission is read-only. It preserves successful retries at the
// limit. The serialized owner can reject known-full submissions before logging;
// ordered application must repeat the check to cover intervening committed writes.
func (m *Machine) CheckCheckpointAdmission(update Update) error {
	_, _, err := m.checkpointCandidate(update)
	return err
}

// ApplyCheckpointable is a versioned replicated-command policy, not a change to
// legacy Apply/replay. It admits new successes only if their entire original-result
// history remains checkpointable. No acknowledged request ID is evicted.
func (m *Machine) ApplyCheckpointable(update Update) (Snapshot, error) {
	next, state, err := m.checkpointCandidate(update)
	if err != nil {
		return Snapshot{}, err
	}
	if next != nil {
		*m = *next
	}
	return state, nil
}

func (m *Machine) checkpointCandidate(update Update) (*Machine, Snapshot, error) {
	e, fresh, err := m.prepare(update)
	if err != nil {
		return nil, Snapshot{}, err
	}
	if !fresh {
		return nil, result(e), nil
	}
	if len(m.requests) >= maxCheckpointRequests {
		return nil, Snapshot{}, ErrCapacity
	}
	next := &Machine{state: result(e), requests: make(map[string]entry, len(m.requests)+1)}
	for id, old := range m.requests {
		next.requests[id] = old
	}
	next.requests[e.Update.RequestID] = e
	// Reserve the largest index/term encodings. Later no-ops, retries, and
	// elections must not make a previously admitted image outgrow its byte budget.
	if _, err := next.Checkpoint(CheckpointPosition{Index: ^uint64(0), Term: ^uint64(0)}); err != nil {
		return nil, Snapshot{}, fmt.Errorf("%w: %v", ErrCapacity, err)
	}
	return next, result(e), nil
}
