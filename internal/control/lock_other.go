//go:build !windows && !linux

package control

import (
	"fmt"
	"os"
)

func lockFile(*os.File) error { return fmt.Errorf("journal locking is supported on Windows and Linux") }
