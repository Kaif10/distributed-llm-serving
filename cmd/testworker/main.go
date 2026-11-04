// Command testworker runs a synthetic streaming backend for local experiments.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kaif10/inference-fleet/internal/testworker"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8081", "HTTP listen address")
	interval := flag.Duration("interval", 300*time.Millisecond, "delay between synthetic chunks (0 < interval <= 1s)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *address, *interval); err != nil {
		slog.Error("test worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, address string, interval time.Duration) error {
	if interval <= 0 || interval > time.Second {
		return fmt.Errorf("interval must be positive and at most 1s")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{
		Handler:           testworker.NewHandler(interval, slog.Default()),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 2*time.Minute + 10*time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 << 10,
		BaseContext:    func(net.Listener) context.Context { return ctx },
	}
	slog.Info("test worker listening", "address", listener.Addr().String(), "synthetic", true)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(drainCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve during shutdown: %w", err)
	}
	return nil
}
