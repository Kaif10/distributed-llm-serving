package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"unicode/utf8"

	"github.com/Kaif10/inference-fleet/internal/filelock"
)

const retainedRecordLimit = 24 << 20

// RetainedIdentity binds this local store to a fixed Raft group/member. It is
// metadata, not authentication. A dedicated directory is required.
type RetainedIdentity struct {
	Group  string `json:"group"`
	Member string `json:"member"`
}

type retainedImage struct {
	Kind       string           `json:"kind"`
	Identity   RetainedIdentity `json:"identity"`
	Generation uint64           `json:"generation"`
	State      HardState        `json:"state"`
	Snapshot   RetainedSnapshot `json:"snapshot"`
	Log        []Entry          `json:"log"`
}

// RetainedStorage implements SnapshotSource using two alternating, bounded full
// images and a stable ownership/identity file. It is not connected to raftpeer.
// The owner must serialize node access, Retain, and construction/reconstruction.
type RetainedStorage struct {
	mu       sync.Mutex
	lock     *os.File
	slots    [2]*os.File
	selected int
	image    retainedImage
	failed   bool
	write    func(int, []byte) (int, error)
	syncSlot func(int) error
	// Test-only process barriers, absent from serving APIs.
	afterClear, afterWrite, afterSync func()
}

func validRetainedIdentity(id RetainedIdentity) bool {
	return len(id.Group) > 0 && len(id.Group) <= 128 && utf8.ValidString(id.Group) && len(id.Member) > 0 && len(id.Member) <= 64 && utf8.ValidString(id.Member)
}

func retainedFrame(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > retainedRecordLimit {
		return nil, fmt.Errorf("retained image exceeds 24 MiB")
	}
	frame := make([]byte, 12+len(data))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(data))
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(frame[:8]))
	copy(frame[12:], data)
	return frame, nil
}

// A short frame is an interrupted, unacknowledged publication. A complete bad
// frame fails closed; it must never cause fallback to an older term/vote.
func readRetainedFrame(f *os.File, limit int64, value any) (bool, error) {
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() > limit+12 {
		return false, fmt.Errorf("retained file exceeds bound")
	}
	if info.Size() < 12 {
		return false, nil
	}
	var header [12]byte
	if _, err := f.ReadAt(header[:], 0); err != nil {
		return false, err
	}
	if crc32.ChecksumIEEE(header[:8]) != binary.LittleEndian.Uint32(header[8:]) {
		return false, fmt.Errorf("retained header checksum mismatch")
	}
	length := int64(binary.LittleEndian.Uint32(header[:4]))
	if length == 0 || length > limit || info.Size() > length+12 {
		return false, fmt.Errorf("invalid retained frame length")
	}
	if info.Size() < length+12 {
		return false, nil
	}
	data := make([]byte, length)
	if _, err := f.ReadAt(data, 12); err != nil {
		return false, err
	}
	if crc32.ChecksumIEEE(data) != binary.LittleEndian.Uint32(header[4:8]) {
		return false, fmt.Errorf("retained payload checksum mismatch")
	}
	if err := json.Unmarshal(data, value); err != nil {
		return false, err
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(data, canonical) {
		return false, fmt.Errorf("noncanonical retained frame")
	}
	return true, nil
}

type retainedOwner struct {
	Kind     string           `json:"kind"`
	Identity RetainedIdentity `json:"identity"`
}

func OpenRetainedStorage(dir string, id RetainedIdentity) (*RetainedStorage, error) {
	return openRetainedStorage(dir, id, nil)
}

func openRetainedStorage(dir string, id RetainedIdentity, syncOverride func(int) error) (*RetainedStorage, error) {
	if !validRetainedIdentity(id) {
		return nil, fmt.Errorf("invalid retained group/member identity")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	ownerPath := filepath.Join(dir, "owner.wal")
	lock, err := os.OpenFile(ownerPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	fresh := err == nil
	if os.IsExist(err) {
		lock, err = os.OpenFile(ownerPath, os.O_RDWR, 0600)
	}
	if err != nil {
		return nil, err
	}
	if err := filelock.Acquire(lock); err != nil {
		lock.Close()
		return nil, fmt.Errorf("exclusive retained storage lock: %w", err)
	}
	s := &RetainedStorage{lock: lock}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()
	s.write = func(slot int, data []byte) (int, error) { return s.slots[slot].WriteAt(data, 0) }
	s.syncSlot = func(slot int) error { return s.slots[slot].Sync() }
	if syncOverride != nil {
		s.syncSlot = syncOverride
	}
	owner := retainedOwner{Kind: "fleet-retained-owner-v1", Identity: id}
	if !fresh {
		var actual retainedOwner
		complete, err := readRetainedFrame(lock, 1024, &actual)
		if err != nil {
			return nil, err
		}
		if !complete || actual != owner {
			return nil, fmt.Errorf("incomplete or mismatched retained owner; refusing reinitialization")
		}
	}
	for i := range s.slots {
		flags := os.O_RDWR
		if fresh {
			flags |= os.O_CREATE | os.O_EXCL
		}
		s.slots[i], err = os.OpenFile(filepath.Join(dir, fmt.Sprintf("image-%d.wal", i)), flags, 0600)
		if err != nil {
			return nil, err
		}
	}
	if fresh {
		s.image = retainedImage{Kind: "fleet-retained-image-v1", Identity: id, Generation: 1, Log: []Entry{}}
		frame, err := retainedFrame(s.image)
		if err != nil {
			return nil, err
		}
		if n, err := s.write(0, frame); err != nil || n != len(frame) {
			return nil, fmt.Errorf("initialize retained image: %v (%d/%d bytes)", err, n, len(frame))
		}
		for i := range s.slots {
			if err := s.syncSlot(i); err != nil {
				return nil, err
			}
		}
		frame, err = retainedFrame(owner)
		if err != nil {
			return nil, err
		}
		if n, err := lock.WriteAt(frame, 0); err != nil || n != len(frame) {
			return nil, fmt.Errorf("initialize retained owner: %v (%d/%d bytes)", err, n, len(frame))
		}
	} else {
		var images [2]retainedImage
		var complete [2]bool
		for i := range s.slots {
			complete[i], err = readRetainedFrame(s.slots[i], retainedRecordLimit, &images[i])
			if err != nil {
				return nil, err
			}
			if complete[i] {
				if err := validRetainedImage(images[i], id); err != nil {
					return nil, err
				}
			}
		}
		if !complete[0] && !complete[1] {
			return nil, fmt.Errorf("no complete retained image")
		}
		if complete[1] && (!complete[0] || images[1].Generation > images[0].Generation) {
			s.selected = 1
		}
		s.image = images[s.selected]
		if complete[0] && complete[1] {
			older := images[1-s.selected]
			if s.image.Generation != older.Generation+1 || older.Generation == ^uint64(0) {
				return nil, fmt.Errorf("nonconsecutive retained generations")
			}
			if err := retainedTransition(older, s.image); err != nil {
				return nil, err
			}
		}
		for i := range s.slots {
			if !complete[i] {
				if err := s.slots[i].Truncate(0); err != nil {
					return nil, err
				}
			}
			// Sync recovered complete-but-uncertain bytes before allowing new votes.
			if err := s.syncSlot(i); err != nil {
				return nil, err
			}
		}
	}
	if err := lock.Sync(); err != nil {
		return nil, err
	}
	ok = true
	return s, nil
}

func validRetainedImage(image retainedImage, id RetainedIdentity) error {
	if image.Kind != "fleet-retained-image-v1" || image.Identity != id || image.Generation == 0 || !utf8.ValidString(image.State.VotedFor) || len(image.State.VotedFor) > 64 {
		return fmt.Errorf("invalid retained image metadata")
	}
	if image.State.Term == 0 && (image.State.VotedFor != "" || image.Snapshot.Index != 0 || len(image.Log) != 0 || image.Generation != 1) {
		return fmt.Errorf("invalid initial retained state")
	}
	if image.Generation == 1 && image.State.Term != 0 {
		return fmt.Errorf("initial retained generation must have zero term")
	}
	return validRetainedLog(image.Snapshot, image.Log, image.State.Term)
}

func retainedTransition(old, next retainedImage) error {
	if !validTransition(old.State, next.State) {
		return fmt.Errorf("term regression or conflicting retained vote")
	}
	if next.Snapshot.Index == old.Snapshot.Index {
		if !reflect.DeepEqual(next.Snapshot, old.Snapshot) {
			return fmt.Errorf("retained boundary changed at same index")
		}
		return nil
	}
	if next.Snapshot.Index < old.Snapshot.Index || next.Snapshot.Index > old.Snapshot.Index+uint64(len(old.Log)) {
		return fmt.Errorf("retained boundary outside previous log")
	}
	offset := next.Snapshot.Index - old.Snapshot.Index
	if old.Log[offset-1].Term != next.Snapshot.Term || !reflect.DeepEqual(cloneEntries(old.Log[offset:]), cloneEntries(next.Log)) {
		return fmt.Errorf("retained boundary term or suffix mismatch")
	}
	return nil
}

func (s *RetainedStorage) Load() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return HardState{}, ErrStorage
	}
	return s.image.State, nil
}
func (s *RetainedStorage) LoadLog() ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return nil, ErrStorage
	}
	return cloneEntries(s.image.Log), nil
}
func (s *RetainedStorage) LoadSnapshot() (RetainedSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return RetainedSnapshot{}, ErrStorage
	}
	return cloneSnapshot(s.image.Snapshot), nil
}
func (s *RetainedStorage) Save(state HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.image
	next.State = state
	return s.publish(next)
}
func (s *RetainedStorage) SaveLog(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.image
	next.Log = cloneEntries(entries)
	return s.publish(next)
}

// Retain atomically replaces a local committed prefix with its application image.
// The serialized owner must certify that applied is committed/applied and that
// the bytes encode exactly this prefix, including original retry results. This
// storage layer checks position/term/suffix, not application semantics. Never call
// it behind a live Node: reconstruct the node and restore the application first.
// Remote snapshot installation needs a separate validated protocol.
func (s *RetainedStorage) Retain(snapshot RetainedSnapshot, applied uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return ErrStorage
	}
	if snapshot.Index <= s.image.Snapshot.Index || snapshot.Index > applied || applied > s.image.Snapshot.Index+uint64(len(s.image.Log)) {
		return fmt.Errorf("retained boundary must advance within committed/applied local log")
	}
	next := s.image
	next.Snapshot = cloneSnapshot(snapshot)
	next.Log = cloneEntries(s.image.Log[snapshot.Index-s.image.Snapshot.Index:])
	return s.publish(next)
}

func (s *RetainedStorage) publish(next retainedImage) error {
	if s.failed {
		return ErrStorage
	}
	if s.image.Generation == ^uint64(0) {
		return fmt.Errorf("retained generation exhausted")
	}
	next.Generation++
	if err := validRetainedImage(next, s.image.Identity); err != nil {
		return err
	}
	if err := retainedTransition(s.image, next); err != nil {
		return err
	}
	frame, err := retainedFrame(next)
	if err != nil {
		return err
	}
	target := 1 - s.selected
	if err = s.slots[target].Truncate(0); err == nil {
		err = s.syncSlot(target)
	}
	if err == nil && s.afterClear != nil {
		s.afterClear()
	}
	if err == nil {
		var n int
		n, err = s.write(target, frame)
		if err == nil && n != len(frame) {
			err = io.ErrShortWrite
		}
	}
	if err == nil && s.afterWrite != nil {
		s.afterWrite()
	}
	if err == nil {
		err = s.syncSlot(target)
	}
	if err != nil {
		s.failed = true
		return fmt.Errorf("%w: retained publication outcome uncertain: %v", ErrStorage, err)
	}
	if s.afterSync != nil {
		s.afterSync()
	}
	s.image = next
	s.selected = target
	return nil
}

func (s *RetainedStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = true
	var first error
	for _, f := range s.slots {
		if f != nil {
			if err := f.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	if err := s.lock.Close(); err != nil && first == nil {
		first = err
	}
	return first
}
