package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMembershipBoundary(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:http", "localhost:9001"} {
		if loopback(address) {
			t.Fatalf("accepted invalid address %s", address)
		}
	}
	for _, spec := range []string{"a=https://127.0.0.1:9001", "a=http://0.0.0.0:9001", "a=http://example.com:9001", "a=http://127.0.0.1:9001/path", "a=http://user@127.0.0.1:9001", "a=http://127.0.0.1:9001?", "b=http://127.0.0.1:9001", "a=http://127.0.0.1:9001,a=http://127.0.0.1:9002", "a=http://127.0.0.1:9001,b=http://127.0.0.1:9001"} {
		if _, err := members("a", spec); err == nil {
			t.Fatalf("accepted %s", spec)
		}
	}
	if _, err := members("a", "a=http://127.0.0.1:9001,b=http://[::1]:9002"); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedStorageModeRejectsUnsafeCombinations(t *testing.T) {
	for _, mode := range []string{"raw", "no-group", "group-only", "both-storage", "no-storage", "checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path, retained, group := "", filepath.Join(dir, "retained"), "test"
			spec, configuration := "a=http://127.0.0.1:19001", true
			checkpointPath, checkpointGroup := "", ""
			switch mode {
			case "raw":
				configuration = false
			case "multi":
				spec += ",b=http://127.0.0.1:19002,c=http://127.0.0.1:19003"
			case "no-group":
				group = ""
			case "group-only":
				retained = ""
			case "both-storage":
				path = filepath.Join(dir, "raft.wal")
			case "no-storage":
				retained, group = "", ""
			case "checkpoint":
				checkpointPath, checkpointGroup = filepath.Join(dir, "checkpoint.wal"), "test"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := run(ctx, "a", spec, "127.0.0.1:19004", path, configuration, "", checkpointPath, checkpointGroup, retained, group); err == nil {
				t.Fatal("unsafe retained-storage mode accepted")
			}
			files, err := os.ReadDir(dir)
			if err != nil || len(files) != 0 {
				t.Fatal("invalid mode reached storage initialization", files, err)
			}
		})
	}
}
