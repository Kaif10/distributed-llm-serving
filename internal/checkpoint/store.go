// Package checkpoint publishes complete application checkpoints in a synced,
// exclusively owned append-only journal. It does not install or compact Raft logs.
package checkpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf8"

	"github.com/Kaif10/inference-fleet/internal/control"
	"github.com/Kaif10/inference-fleet/internal/filelock"
)

const (
	maxRecord      = 12 << 20
	maxGenerations = 16
)

var ErrUnavailable = errors.New("checkpoint publication store unavailable; publication outcome may be unknown")
var ErrFull = errors.New("checkpoint publication journal reached 16 generations")

type Identity struct {
	Group  string `json:"group"`
	Member string `json:"member"`
}
type Selection struct {
	Generation uint64                     `json:"generation"`
	Identity   Identity                   `json:"identity"`
	Position   control.CheckpointPosition `json:"position"`
	SHA256     string                     `json:"sha256"`
	Data       []byte                     `json:"-"`
}
type record struct {
	Kind string `json:"kind"`
	Selection
	Data []byte `json:"checkpoint"`
}
type Store struct {
	mu                     sync.Mutex
	file                   *os.File
	identity               Identity
	latest                 Selection
	machine                *control.Machine
	failed, closed         bool
	writeFrame             func([]byte) (int, error)
	syncFile               func() error
	afterAppend, afterSync func() // Internal deterministic/process fault-test boundaries.
}

func validIdentity(id Identity) bool {
	return len(id.Group) > 0 && len(id.Group) <= 128 && len(id.Member) > 0 && len(id.Member) <= 128 && utf8.ValidString(id.Group) && utf8.ValidString(id.Member)
}
func clone(s Selection) Selection { s.Data = bytes.Clone(s.Data); return s }
func digest(data []byte) string   { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func Open(path string, id Identity) (*Store, error) { return open(path, id, nil) }

// Identity is immutable metadata, available even before the first publication.
func (s *Store) Identity() Identity { return s.identity }
func open(path string, id Identity, syncOverride func(*os.File) error) (*Store, error) {
	if !validIdentity(id) {
		return nil, fmt.Errorf("checkpoint store requires group/member IDs of 1..128 UTF-8 bytes")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := filelock.Acquire(file); err != nil {
		file.Close()
		return nil, fmt.Errorf("exclusive checkpoint publication lock: %w", err)
	}
	s := &Store{file: file, identity: id, writeFrame: file.Write, syncFile: file.Sync}
	if syncOverride != nil {
		s.syncFile = func() error { return syncOverride(file) }
	}
	if err := s.recover(); err != nil {
		file.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) validate(r record) (*control.Machine, error) {
	if r.Kind != "fleet-publication-v1" || r.Identity != s.identity || r.Generation != s.latest.Generation+1 || r.Generation > maxGenerations || r.SHA256 != digest(r.Data) {
		return nil, fmt.Errorf("invalid checkpoint publication identity, generation, or digest")
	}
	machine, position, err := control.RestoreCheckpoint(r.Data)
	if err != nil {
		return nil, err
	}
	if r.Position != position {
		return nil, fmt.Errorf("checkpoint publication position mismatch")
	}
	if s.machine != nil {
		if position.Index <= s.latest.Position.Index || position.Term < s.latest.Position.Term {
			return nil, fmt.Errorf("checkpoint publication position regressed or repeated")
		}
		if err := machine.ValidateSuccessor(s.machine); err != nil {
			return nil, err
		}
	}
	return machine, nil
}
func frameRecord(r record) ([]byte, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || len(payload) > maxRecord {
		return nil, fmt.Errorf("checkpoint publication exceeds 12 MiB")
	}
	frame := make([]byte, 12+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], payload)
	return frame, nil
}
func (s *Store) recover() error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > int64(maxGenerations*(maxRecord+12)) {
		return fmt.Errorf("checkpoint publication journal exceeds bound")
	}
	var offset int64
	for {
		var header [12]byte
		n, err := io.ReadFull(s.file, header[:])
		if err == io.EOF && n == 0 {
			break
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if err := s.file.Truncate(offset); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(header[:8]) != binary.LittleEndian.Uint32(header[8:]) {
			return fmt.Errorf("checkpoint publication header checksum mismatch")
		}
		length := binary.LittleEndian.Uint32(header[:4])
		if length == 0 || length > maxRecord {
			return fmt.Errorf("invalid checkpoint publication length")
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(s.file, payload); err == io.EOF || err == io.ErrUnexpectedEOF {
			if err := s.file.Truncate(offset); err != nil {
				return err
			}
			break
		} else if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(header[4:]) {
			return fmt.Errorf("checkpoint publication payload checksum mismatch")
		}
		var r record
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&r); err != nil {
			return err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return fmt.Errorf("expected one checkpoint publication")
		}
		canonical, err := json.Marshal(r)
		if err != nil || !bytes.Equal(canonical, payload) {
			return fmt.Errorf("noncanonical checkpoint publication")
		}
		machine, err := s.validate(r)
		if err != nil {
			return err
		}
		r.Selection.Data = r.Data
		s.latest = r.Selection
		s.machine = machine
		offset += 12 + int64(length)
	}
	// A complete but unacknowledged append is eligible only after this barrier.
	if err := s.syncFile(); err != nil {
		return err
	}
	_, err = s.file.Seek(0, io.SeekEnd)
	return err
}

func (s *Store) Latest() (Selection, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed {
		return Selection{}, false, ErrUnavailable
	}
	return clone(s.latest), s.latest.Generation != 0, nil
}
func (s *Store) Publish(data []byte) (Selection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed {
		return Selection{}, ErrUnavailable
	}
	// The input is owned by this publication; no caller buffer is retained.
	if len(data) > (8<<20)+12 {
		return Selection{}, fmt.Errorf("application checkpoint exceeds bound")
	}
	candidate := bytes.Clone(data)
	if s.latest.Generation != 0 && bytes.Equal(candidate, s.latest.Data) {
		return clone(s.latest), nil
	}
	if s.latest.Generation >= maxGenerations {
		return Selection{}, ErrFull
	}
	_, position, err := control.RestoreCheckpoint(candidate)
	if err != nil {
		return Selection{}, err
	}
	selection := Selection{Generation: s.latest.Generation + 1, Identity: s.identity, Position: position, SHA256: digest(candidate)}
	r := record{Kind: "fleet-publication-v1", Selection: selection, Data: candidate}
	machine, err := s.validate(r)
	if err != nil {
		return Selection{}, err
	}
	frame, err := frameRecord(r)
	if err != nil {
		return Selection{}, err
	}
	n, err := s.writeFrame(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.failed = true
		return Selection{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if s.afterAppend != nil {
		s.afterAppend()
	}
	if err := s.syncFile(); err != nil {
		s.failed = true
		return Selection{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if s.afterSync != nil {
		s.afterSync()
	}
	selection.Data = candidate
	s.latest = selection
	s.machine = machine
	return clone(selection), nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}
