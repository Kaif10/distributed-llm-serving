package control

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"reflect"
	"sort"
)

const (
	maxCheckpointPayload  = 8 << 20
	maxCheckpointRequests = 1024
)

// CheckpointPosition is supplied by the serialized application owner, after
// applying a committed prefix. The codec cannot prove consensus or group identity.
// A future installer must bind this position to the trusted Raft state before use.
type CheckpointPosition struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}
type checkpointPayload struct {
	Kind     string             `json:"kind"`
	Position CheckpointPosition `json:"position"`
	State    Snapshot           `json:"state"`
	Requests []entry            `json:"requests"`
}

// ValidateSuccessor prevents a later checkpoint from discarding or changing an
// already retained successful request/result. Position and group binding remain
// the publisher's responsibility. Callers serialize access to both machines.
func (m *Machine) ValidateSuccessor(previous *Machine) error {
	if previous == nil || m.state.Revision < previous.state.Revision {
		return fmt.Errorf("checkpoint configuration regressed")
	}
	for id, old := range previous.requests {
		next, ok := m.requests[id]
		if !ok || !reflect.DeepEqual(next, old) {
			return fmt.Errorf("checkpoint changed retained successful history")
		}
	}
	return nil
}

func validCheckpointPosition(position CheckpointPosition, revision uint64) bool {
	return (position.Index == 0) == (position.Term == 0) && revision <= position.Index
}

// Checkpoint returns one canonical, checksummed application checkpoint, including
// every successful request's original update/result. It does not persist, publish,
// install, compact, or certify the supplied committed/applied position.
func (m *Machine) Checkpoint(position CheckpointPosition) ([]byte, error) {
	if !validCheckpointPosition(position, m.state.Revision) || len(m.requests) > maxCheckpointRequests || uint64(len(m.requests)) != m.state.Revision {
		return nil, fmt.Errorf("invalid checkpoint position or successful-request count")
	}
	requests := make([]entry, 0, len(m.requests))
	// Bound raw string content before JSON allocation. Escaping can still expand
	// this bounded input; the final encoded payload must also fit the wire limit.
	rawBytes := 0
	addWorkers := func(workers []Worker) bool {
		for _, w := range workers {
			if len(w.ID) > maxCheckpointPayload-rawBytes {
				return false
			}
			rawBytes += len(w.ID)
			if len(w.Origin) > maxCheckpointPayload-rawBytes {
				return false
			}
			rawBytes += len(w.Origin)
		}
		return true
	}
	if !addWorkers(m.state.Workers) {
		return nil, fmt.Errorf("checkpoint exceeds raw payload budget")
	}
	for id, e := range m.requests {
		if id != e.Update.RequestID || len(id) > maxCheckpointPayload-rawBytes {
			return nil, fmt.Errorf("invalid checkpoint request or size")
		}
		rawBytes += len(id)
		if !addWorkers(e.Update.Workers) {
			return nil, fmt.Errorf("checkpoint exceeds raw payload budget")
		}
		if err := ValidateUpdate(e.Update); err != nil {
			return nil, err
		}
		e.Update.Workers = cloneWorkers(e.Update.Workers)
		requests = append(requests, e)
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].Revision < requests[j].Revision })
	for i, e := range requests {
		if e.Revision != uint64(i)+1 || e.Update.ExpectedRevision != uint64(i) {
			return nil, fmt.Errorf("noncontiguous successful request history")
		}
	}
	want := Snapshot{Workers: []Worker{}}
	if len(requests) > 0 {
		want = result(requests[len(requests)-1])
	}
	if !reflect.DeepEqual(want, m.Snapshot()) {
		return nil, fmt.Errorf("checkpoint state disagrees with successful history")
	}
	payload, err := json.Marshal(checkpointPayload{Kind: "fleet-application-v1", Position: position, State: m.Snapshot(), Requests: requests})
	if err != nil {
		return nil, err
	}
	if len(payload) > maxCheckpointPayload {
		return nil, fmt.Errorf("checkpoint exceeds eight MiB")
	}
	frame := make([]byte, 12+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], payload)
	return frame, nil
}

// RestoreCheckpoint builds a fresh machine or returns no machine on any error.
// It validates framing, version, successful-history order, CAS transitions, state,
// and canonical encoding. Position remains untrusted until the owner binds it to
// its committed log; a CRC is corruption detection, not authentication.
func RestoreCheckpoint(frame []byte) (*Machine, CheckpointPosition, error) {
	fail := func(message string) (*Machine, CheckpointPosition, error) {
		return nil, CheckpointPosition{}, fmt.Errorf("invalid application checkpoint: %s", message)
	}
	if len(frame) < 12 || len(frame) > 12+maxCheckpointPayload {
		return fail("frame size")
	}
	if crc32.ChecksumIEEE(frame[:8]) != binary.LittleEndian.Uint32(frame[8:12]) {
		return fail("header checksum")
	}
	length := binary.LittleEndian.Uint32(frame[:4])
	if length == 0 || uint64(length) != uint64(len(frame)-12) {
		return fail("payload length")
	}
	payload := frame[12:]
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(frame[4:8]) {
		return fail("payload checksum")
	}
	var recorded checkpointPayload
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&recorded); err != nil {
		return fail(err.Error())
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fail("expected one payload object")
	}
	if recorded.Kind != "fleet-application-v1" || len(recorded.Requests) > maxCheckpointRequests || uint64(len(recorded.Requests)) != recorded.State.Revision || !validCheckpointPosition(recorded.Position, recorded.State.Revision) {
		return fail("version, position, or successful-request count")
	}
	machine := NewMachine()
	for i, e := range recorded.Requests {
		if e.Revision != uint64(i)+1 || e.Update.ExpectedRevision != uint64(i) {
			return fail("successful history order")
		}
		if _, exists := machine.requests[e.Update.RequestID]; exists {
			return fail("duplicate successful request ID")
		}
		state, err := machine.Apply(e.Update)
		if err != nil || state.Revision != e.Revision {
			return fail("successful history transition")
		}
	}
	if !reflect.DeepEqual(machine.Snapshot(), recorded.State) {
		return fail("state disagrees with successful history")
	}
	canonical, err := machine.Checkpoint(recorded.Position)
	if err != nil {
		return fail(err.Error())
	}
	if !bytes.Equal(canonical, frame) {
		return fail("noncanonical encoding")
	}
	return machine, recorded.Position, nil
}
