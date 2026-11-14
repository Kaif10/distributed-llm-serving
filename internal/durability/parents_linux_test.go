package durability

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewNestedDirectoryPublication(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "parent", "store")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "vote.wal")
	if err := os.WriteFile(path, []byte("synced by owner before publishing names"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SyncParents(path); err != nil {
		t.Fatal(err)
	}
	if err := SyncParents(filepath.Join(dir, "absent", "vote.wal")); err == nil {
		t.Fatal("missing directory sync failure hidden")
	}
}
