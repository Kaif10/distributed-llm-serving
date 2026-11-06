package filelock

import (
	"os"
	"syscall"
	"unsafe"
)

// Windows releases this byte-range lock when the process/file handle closes.
func Acquire(file *os.File) error {
	var overlapped syscall.Overlapped
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")
	ok, _, err := proc.Call(file.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if ok == 0 {
		return err
	}
	return nil
}
