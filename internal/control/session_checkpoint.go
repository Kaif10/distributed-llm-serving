package control

import (
	"fmt"
	"reflect"
	"sort"
)

func (m *Machine) sessionCheckpoint(position CheckpointPosition) ([]byte, error) {
	r := checkpointPayload{Kind: "fleet-application-v2", Position: position, State: m.Snapshot(), Requests: make([]entry, 0, len(m.requests))}
	for _, e := range m.requests {
		r.Requests = append(r.Requests, e)
	}
	sort.Slice(r.Requests, func(i, j int) bool { return r.Requests[i].Revision < r.Requests[j].Revision })
	for _, session := range m.sessions {
		if session.Generation > 0 {
			r.Sessions = append(r.Sessions, session)
		}
	}
	if _, err := restoreSessionPayload(r); err != nil {
		return nil, err
	}
	return frameCheckpoint(r)
}

// Expired intermediate session results are intentionally absent. Unlike v1,
// this schema validates the bounded retained results and durable fences; Raft
// binds the authoritative current state to its committed checkpoint position.
func restoreSessionPayload(r checkpointPayload) (*Machine, error) {
	if r.Kind != "fleet-application-v2" || !validCheckpointPosition(r.Position, r.State.Revision) || len(r.Requests) > maxCheckpointRequests || len(r.Sessions) == 0 || len(r.Sessions) > SessionSlots {
		return nil, fmt.Errorf("invalid session image metadata")
	}
	if err := ValidateWorkers(r.State.Workers); err != nil {
		return nil, err
	}
	m := NewMachine()
	m.sessionsEnabled = true
	m.state = Snapshot{Revision: r.State.Revision, Workers: cloneWorkers(r.State.Workers)}
	seen := map[uint64]bool{}
	validateResult := func(e entry) error {
		if err := ValidateUpdate(e.Update); err != nil {
			return err
		}
		if e.Revision == 0 || e.Revision > r.State.Revision || e.Update.ExpectedRevision == ^uint64(0) || e.Update.ExpectedRevision+1 != e.Revision || seen[e.Revision] {
			return fmt.Errorf("invalid retained result revision")
		}
		seen[e.Revision] = true
		if e.Revision == r.State.Revision && !reflect.DeepEqual(result(e), m.Snapshot()) {
			return fmt.Errorf("latest retained result differs from current state")
		}
		return nil
	}
	var lastRevision uint64
	for _, e := range r.Requests {
		if e.Revision <= lastRevision {
			return nil, fmt.Errorf("legacy results not ordered")
		}
		if _, exists := m.requests[e.Update.RequestID]; exists {
			return nil, fmt.Errorf("duplicate legacy request")
		}
		if err := validateResult(e); err != nil {
			return nil, err
		}
		e.Update.Workers = cloneWorkers(e.Update.Workers)
		m.requests[e.Update.RequestID] = e
		lastRevision = e.Revision
	}
	lastSlot := -1
	for _, s := range r.Sessions {
		if s.Slot <= lastSlot || s.Slot >= SessionSlots || s.Generation == 0 || s.OpenExpected == ^uint64(0) || s.OpenExpected+1 != s.Generation {
			return nil, fmt.Errorf("invalid session slot/generation")
		}
		if err := ValidateSessionOpen(SessionOpen{Slot: s.Slot, ExpectedGeneration: s.OpenExpected, RequestID: s.OpenRequestID}); err != nil {
			return nil, err
		}
		if (s.Sequence == 0) != (s.Latest == nil) {
			return nil, fmt.Errorf("invalid session result fence")
		}
		if s.Latest != nil {
			if s.Latest.Update.RequestID != "session-update" || s.Sequence > s.Latest.Revision {
				return nil, fmt.Errorf("invalid session latest result")
			}
			if err := validateResult(*s.Latest); err != nil {
				return nil, err
			}
			e := *s.Latest
			e.Update.Workers = cloneWorkers(e.Update.Workers)
			s.Latest = &e
		}
		m.sessions[s.Slot] = s
		lastSlot = s.Slot
	}
	return m, nil
}
