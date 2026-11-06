//go:build !windows && !linux

package filelock

import (
	"fmt"
	"os"
)

func Acquire(*os.File) error { return fmt.Errorf("journal locking is supported on Windows and Linux") }
