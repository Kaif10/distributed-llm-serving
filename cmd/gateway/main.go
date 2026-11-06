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
	"strings"
	"syscall"
	"time"

	"github.com/Kaif10/inference-fleet/internal/gateway"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8080", "HTTP listen address")
	backend := flag.String("backend", "", "worker origin, e.g. http://127.0.0.1:8081")
	backends := flag.String("backends", "", "comma-separated worker origins serving the same model")
	policy := flag.String("policy", "least-inflight", "least-inflight, round-robin, or prefix-affinity")
	limit := flag.Int("max-inflight", 1, "maximum admitted requests per worker, local to this gateway")
	admin := flag.String("admin-listen", "", "optional loopback-only administration address")
	controller := flag.String("controller", "", "optional controller origin for durable drain settings")
	configPoll := flag.Duration("config-poll", 500*time.Millisecond, "controller polling interval")
	configTimeout := flag.Duration("config-timeout", time.Second, "controller fetch timeout")
	configAge := flag.Duration("config-max-age", 5*time.Second, "maximum age of a confirmed controller view")
	healthPath := flag.String("health-path", "/health", "worker readiness probe path")
	healthInterval := flag.Duration("health-interval", time.Second, "delay between worker probes")
	healthTimeout := flag.Duration("health-timeout", 500*time.Millisecond, "per-probe timeout")
	timeout := flag.Duration("request-timeout", 30*time.Second, "forwarding deadline; downstream writes bounded by this plus 1s")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *backend != "" && *backends != "" {
		slog.Error("use either -backend or -backends")
		os.Exit(1)
	}
	var origins []string
	if *backend != "" {
		origins = []string{*backend}
	}
	if *backends != "" {
		for _, origin := range strings.Split(*backends, ",") {
			origins = append(origins, strings.TrimSpace(origin))
		}
	}
	options := gateway.PoolOptions{Backends: origins, Policy: *policy, MaxInflight: *limit}
	config := gateway.ControllerOptions{Origin: *controller, Interval: *configPoll, Timeout: *configTimeout, MaxAge: *configAge}
	if err := run(ctx, *address, *admin, options, *timeout, *healthInterval, *healthTimeout, *healthPath, config); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, address, admin string, options gateway.PoolOptions, timeout, healthInterval, healthTimeout time.Duration, healthPath string, config gateway.ControllerOptions) error {
	if admin != "" {
		host, _, err := net.SplitHostPort(admin)
		if err != nil || !net.ParseIP(host).IsLoopback() {
			return fmt.Errorf("admin listener must use a loopback IP address")
		}
	}
	handler, err := gateway.NewPoolHandler(options, timeout, slog.Default())
	if err != nil {
		return err
	}
	stopHealth, err := handler.StartHealthChecks(ctx, healthInterval, healthTimeout, healthPath)
	if err != nil {
		return err
	}
	defer stopHealth()
	if config.Origin != "" {
		stopConfig, err := handler.StartController(ctx, config)
		if err != nil {
			return err
		}
		defer stopConfig()
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
	servers := []*http.Server{server}
	listeners := []net.Listener{listener}
	if admin != "" {
		adminListener, err := net.Listen("tcp", admin)
		if err != nil {
			listener.Close()
			return fmt.Errorf("admin listen: %w", err)
		}
		servers = append(servers, &http.Server{Handler: handler.AdminHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10})
		listeners = append(listeners, adminListener)
		slog.Info("gateway admin listening", "address", adminListener.Addr().String())
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()
	slog.Info("gateway listening", "address", listener.Addr().String())
	served := make(chan error, len(servers))
	for i, s := range servers {
		go func() { served <- s.Serve(listeners[i]) }()
	}
	select {
	case err := <-served:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	slog.Info("gateway draining")
	// The signal context is already cancelled. Give existing requests a new grace period.
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, s := range servers {
		if err := s.Shutdown(drainCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
	}
	for range servers {
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve during shutdown: %w", err)
		}
	}
	return nil
}
