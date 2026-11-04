// Command gateway forwards streaming requests to one configured inference worker.
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

	"github.com/Kaif10/inference-fleet/internal/gateway"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	backend := flag.String("backend", "", "worker origin, e.g. http://127.0.0.1:8081")
	timeout := flag.Duration("request-timeout", 30*time.Second, "forwarding deadline; downstream writes bounded by this plus 1s")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *address, *backend, *timeout); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, address, backend string, timeout time.Duration) error {
	handler, err := gateway.NewHandler(backend, timeout, slog.Default())
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		// The generation handler replaces the short default with its stream deadline.
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 16 << 10,
	}
	// Bind before logging so an occupied port fails visibly at startup.
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	slog.Info("gateway listening", "address", listener.Addr().String())
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	slog.Info("gateway draining")
	// The signal context is already cancelled. Give existing requests a new grace period.
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
