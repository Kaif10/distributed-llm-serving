package main

import "testing"

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
