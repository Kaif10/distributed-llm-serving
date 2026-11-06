package filelock

import (
	"os"
	"syscall"
)

func Acquire(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
