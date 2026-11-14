package durability

import (
	"os"
	"path/filepath"
)

// SyncParents persists names created by MkdirAll/OpenFile before a store can
// acknowledge data. Existing fixed-name file updates still use their file syncs.
// Walk bottom-up because newly created ancestor directories also need publication.
func SyncParents(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for directory := filepath.Dir(absolute); ; directory = filepath.Dir(directory) {
		file, err := os.Open(directory)
		if err != nil {
			return err
		}
		err = file.Sync()
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if filepath.Dir(directory) == directory {
			return nil
		}
	}
}
