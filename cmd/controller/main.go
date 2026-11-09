// Command controller owns a single durable configuration journal.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/control"
)

func main() {
	address := flag.String("listen", "127.0.0.1:19090", "loopback configuration API")
	path := flag.String("journal", "artifacts/controller/fleet.wal", "exclusive local journal path")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *address, *path); err != nil {
		slog.Error("controller stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, address, path string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("controller must listen on a loopback IP")
	}
	store, err := control.Open(path)
	if err != nil {
		return err
	}
	defer store.Close()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: store.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	defer server.Close()
	slog.Info("controller listening", "address", listener.Addr().String())
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(drain)
}
