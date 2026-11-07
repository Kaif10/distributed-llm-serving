package control

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"unicode/utf8"
)

var ErrConflict = errors.New("revision or request ID conflict")

type Worker struct {
	ID       string `json:"id"`
	Origin   string `json:"origin"`
	Draining bool   `json:"draining"`
}
type Snapshot struct {
	Revision uint64   `json:"revision"`
	Workers  []Worker `json:"workers"`
}
type Update struct {
	RequestID        string   `json:"request_id"`
	ExpectedRevision uint64   `json:"expected_revision"`
	Workers          []Worker `json:"workers"`
}
type entry struct {
	Update   Update `json:"update"`
	Revision uint64 `json:"revision"`
}

// Machine makes deterministic decisions for an ordered command stream. It has no
// clock, network, disk, or goroutines. Callers must serialize access. Apply is not
// a durability or consensus acknowledgement. Store owns the sync barriers and
// publishes recovered state only after recovery succeeds.
type Machine struct {
	state    Snapshot
	requests map[string]entry
}

func NewMachine() *Machine {
	return &Machine{state: Snapshot{Workers: []Worker{}}, requests: make(map[string]entry)}
}
func cloneWorkers(workers []Worker) []Worker { return append([]Worker{}, workers...) }
func (m *Machine) Snapshot() Snapshot {
	return Snapshot{Revision: m.state.Revision, Workers: cloneWorkers(m.state.Workers)}
}
func result(e entry) Snapshot {
	return Snapshot{Revision: e.Revision, Workers: cloneWorkers(e.Update.Workers)}
}

// prepare is read-only. Store uses it under its mutex to avoid journaling retries
// and conflicts. Replicated callers must still evaluate CAS at apply time.
func (m *Machine) prepare(u Update) (entry, bool, error) {
	if err := validate(u); err != nil {
		return entry{}, false, err
	}
	u.Workers = cloneWorkers(u.Workers)
	if previous, ok := m.requests[u.RequestID]; ok {
		a, _ := json.Marshal(previous.Update)
		b, _ := json.Marshal(u)
		if string(a) != string(b) {
			return entry{}, false, ErrConflict
		}
		return previous, false, nil
	}
	if u.ExpectedRevision != m.state.Revision {
		return entry{}, false, ErrConflict
	}
	if m.state.Revision == ^uint64(0) {
		return entry{}, false, fmt.Errorf("revision exhausted")
	}
	return entry{Update: u, Revision: m.state.Revision + 1}, true, nil
}

// Apply evaluates even failed commands deterministically. Conflicts and retries
// do not advance configuration revision; successful new updates advance it once.
func (m *Machine) Apply(u Update) (Snapshot, error) {
	e, fresh, err := m.prepare(u)
	if err != nil {
		return Snapshot{}, err
	}
	if fresh {
		m.state = result(e)
		m.requests[e.Update.RequestID] = e
	}
	return result(e), nil
}

func ValidateWorkers(workers []Worker) error {
	return validate(Update{RequestID: "snapshot", Workers: workers})
}

// ValidateUpdate checks shape only. Revision and retry decisions belong at apply time.
func ValidateUpdate(update Update) error { return validate(update) }
func validate(u Update) error {
	if !utf8.ValidString(u.RequestID) {
		return fmt.Errorf("request ID must be valid UTF-8")
	}
	if len(u.RequestID) < 1 || len(u.RequestID) > 128 || len(u.Workers) > 128 {
		return fmt.Errorf("require request ID of 1..128 bytes and at most 128 workers")
	}
	ids, origins := map[string]bool{}, map[string]bool{}
	for _, w := range u.Workers {
		if !utf8.ValidString(w.ID) || !utf8.ValidString(w.Origin) {
			return fmt.Errorf("worker fields must be valid UTF-8")
		}
		origin, err := url.Parse(w.Origin)
		if w.ID == "" || len(w.ID) > 128 || ids[w.ID] || err != nil || origin == nil || (origin.Scheme != "http" && origin.Scheme != "https") || origin.Hostname() == "" || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" || origins[w.Origin] {
			return fmt.Errorf("invalid/duplicate worker ID or origin; use an http(s) origin without path or credentials")
		}
		ids[w.ID], origins[w.Origin] = true, true
	}
	return nil
}
