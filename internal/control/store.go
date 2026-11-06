// Package control stores ordered fleet configuration updates on one local disk.
package control

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
)

var ErrConflict = errors.New("revision or request ID conflict")
var ErrUnavailable = errors.New("store unavailable; update outcome may be unknown")

const maxRecord = 1 << 20

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
type Store struct {
	mu       sync.Mutex
	file     *os.File
	state    Snapshot
	requests map[string]entry
	poisoned bool
	syncFile func() error
}

func validate(u Update) error {
	if len(u.RequestID) < 1 || len(u.RequestID) > 128 || len(u.Workers) > 128 {
		return fmt.Errorf("require request ID of 1..128 bytes and at most 128 workers")
	}
	ids, origins := map[string]bool{}, map[string]bool{}
	for _, w := range u.Workers {
		origin, err := url.Parse(w.Origin)
		if w.ID == "" || len(w.ID) > 128 || ids[w.ID] || err != nil || origin == nil || (origin.Scheme != "http" && origin.Scheme != "https") || origin.Hostname() == "" || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" || origins[w.Origin] {
			return fmt.Errorf("invalid/duplicate worker ID or origin; use an http(s) origin without path or credentials")
		}
		ids[w.ID], origins[w.Origin] = true, true
	}
	return nil
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("exclusive journal lock: %w", err)
	}
	s := &Store{file: f, state: Snapshot{Workers: []Worker{}}, requests: make(map[string]entry), syncFile: f.Sync}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) recover() error {
	var offset int64
	for {
		var header [12]byte
		n, err := io.ReadFull(s.file, header[:])
		if err == io.EOF && n == 0 {
			break
		}
		if err == io.ErrUnexpectedEOF {
			return s.trimTail(offset)
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(header[:8]) != binary.LittleEndian.Uint32(header[8:]) {
			return fmt.Errorf("journal header checksum mismatch at %d", offset)
		}
		length := binary.LittleEndian.Uint32(header[:4])
		if length == 0 || length > maxRecord {
			return fmt.Errorf("invalid journal frame at %d", offset)
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(s.file, data); err == io.EOF || err == io.ErrUnexpectedEOF {
			return s.trimTail(offset)
		} else if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(header[4:]) {
			return fmt.Errorf("journal checksum mismatch at %d", offset)
		}
		var e entry
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		if err := validate(e.Update); err != nil {
			return err
		}
		if e.Revision != s.state.Revision+1 || e.Update.ExpectedRevision != s.state.Revision {
			return fmt.Errorf("nonsequential journal revision")
		}
		if _, exists := s.requests[e.Update.RequestID]; exists {
			return fmt.Errorf("duplicate journal request ID")
		}
		s.install(e)
		offset += int64(12 + length)
	}
	_, err := s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *Store) trimTail(offset int64) error {
	if err := s.file.Truncate(offset); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	_, err := s.file.Seek(offset, io.SeekStart)
	return err
}

func (s *Store) install(e entry) {
	s.state = Snapshot{Revision: e.Revision, Workers: append([]Worker{}, e.Update.Workers...)}
	s.requests[e.Update.RequestID] = e
}
func (s *Store) Read() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return Snapshot{}, ErrUnavailable
	}
	return Snapshot{Revision: s.state.Revision, Workers: append([]Worker{}, s.state.Workers...)}, nil
}

func (s *Store) Apply(u Update) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return Snapshot{}, ErrUnavailable
	}
	if err := validate(u); err != nil {
		return Snapshot{}, err
	}
	u.Workers = append([]Worker{}, u.Workers...)
	if previous, ok := s.requests[u.RequestID]; ok {
		a, _ := json.Marshal(previous.Update)
		b, _ := json.Marshal(u)
		if string(a) != string(b) {
			return Snapshot{}, ErrConflict
		}
		return Snapshot{Revision: previous.Revision, Workers: append([]Worker{}, previous.Update.Workers...)}, nil
	}
	if u.ExpectedRevision != s.state.Revision {
		return Snapshot{}, ErrConflict
	}
	e := entry{Update: u, Revision: s.state.Revision + 1}
	data, err := json.Marshal(e)
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) > maxRecord {
		return Snapshot{}, fmt.Errorf("update too large")
	}
	frame := make([]byte, 12+len(data))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(data))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], data)
	n, err := s.file.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.syncFile()
	}
	if err != nil {
		s.poisoned = true
		return Snapshot{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	s.install(e)
	return Snapshot{Revision: s.state.Revision, Workers: append([]Worker{}, s.state.Workers...)}, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poisoned = true
	return s.file.Close()
}
