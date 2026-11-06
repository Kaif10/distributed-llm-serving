// Package control stores ordered fleet configuration updates on one local disk.
package control

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Kaif10/inference-fleet/internal/filelock"
)

var ErrUnavailable = errors.New("store unavailable; update outcome may be unknown")

const maxRecord = 1 << 20

type Store struct {
	mu       sync.Mutex
	file     *os.File
	machine  *Machine
	poisoned bool
	syncFile func() error
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := filelock.Acquire(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("exclusive journal lock: %w", err)
	}
	s := &Store{file: f, machine: NewMachine(), syncFile: f.Sync}
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
		candidate, fresh, err := s.machine.prepare(e.Update)
		if err != nil || !fresh || candidate.Revision != e.Revision {
			return fmt.Errorf("invalid journal command or revision at %d", offset)
		}
		if _, err := s.machine.Apply(e.Update); err != nil {
			return err
		}
		offset += int64(12 + length)
	}
	// A complete record may have survived an unacknowledged or failed sync.
	// Make replayed state durable before Open publishes reads or cached retries.
	if err := s.syncFile(); err != nil {
		return err
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

func (s *Store) Read() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return Snapshot{}, ErrUnavailable
	}
	return s.machine.Snapshot(), nil
}

func (s *Store) Apply(u Update) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisoned {
		return Snapshot{}, ErrUnavailable
	}
	e, fresh, err := s.machine.prepare(u)
	if err != nil {
		return Snapshot{}, err
	}
	if !fresh {
		return result(e), nil
	}
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
	state, err := s.machine.Apply(e.Update)
	if err != nil || state.Revision != e.Revision {
		s.poisoned = true
		return Snapshot{}, fmt.Errorf("%w: persisted command did not apply at expected revision", ErrUnavailable)
	}
	return state, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poisoned = true
	return s.file.Close()
}
