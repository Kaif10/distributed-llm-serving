package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf8"

	"github.com/Kaif10/inference-fleet/internal/filelock"
)

// FileStorage persists only election term/vote. This is not a replicated log.
type FileStorage struct {
	mu       sync.Mutex
	file     *os.File
	state    HardState
	failed   bool
	syncFile func() error
}
type stateRecord struct {
	Kind  string    `json:"kind"`
	State HardState `json:"state"`
}

func validTransition(previous, next HardState) bool {
	return utf8.ValidString(next.VotedFor) && len(next.VotedFor) <= 64 && next.Term > 0 && next.Term >= previous.Term && (next.Term != previous.Term || previous.VotedFor == "" || next.VotedFor == previous.VotedFor)
}
func OpenStorage(path string) (*FileStorage, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := filelock.Acquire(f); err != nil {
		f.Close()
		return nil, err
	}
	s := &FileStorage{file: f, syncFile: f.Sync}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}
func (s *FileStorage) recover() error {
	var offset int64
	for {
		var header [12]byte
		n, err := io.ReadFull(s.file, header[:])
		if err == io.EOF && n == 0 {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if err := s.file.Truncate(offset); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(header[:8]) != binary.LittleEndian.Uint32(header[8:]) {
			return fmt.Errorf("election header checksum mismatch")
		}
		length := binary.LittleEndian.Uint32(header[:4])
		if length == 0 || length > 1024 {
			return fmt.Errorf("invalid election record length")
		}
		data := make([]byte, length)
		_, err = io.ReadFull(s.file, data)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if err := s.file.Truncate(offset); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(header[4:]) {
			return fmt.Errorf("election payload checksum mismatch")
		}
		var record stateRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		if record.Kind != "raft-election-v1" || !validTransition(s.state, record.State) {
			return fmt.Errorf("invalid election state transition")
		}
		s.state = record.State
		offset += int64(12 + length)
	}
	if err := s.syncFile(); err != nil {
		return err
	}
	_, err := s.file.Seek(0, io.SeekEnd)
	return err
}
func (s *FileStorage) Load() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return HardState{}, ErrStorage
	}
	return s.state, nil
}
func (s *FileStorage) Save(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return ErrStorage
	}
	if !validTransition(s.state, state) {
		return fmt.Errorf("term regression or conflicting durable vote")
	}
	data, err := json.Marshal(stateRecord{Kind: "raft-election-v1", State: state})
	if err != nil {
		return err
	}
	if len(data) > 1024 {
		return fmt.Errorf("election record too large")
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
		s.failed = true
		return err
	}
	s.state = state
	return nil
}
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = true
	return s.file.Close()
}
