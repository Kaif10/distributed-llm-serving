// Package gateway forwards one bounded request to a statically configured worker.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const maxRequestBytes = 64 << 10

// NewHandler accepts a backend origin (no path/query) and a total forwarding deadline.
// An empty backend keeps health available and rejects generation with 503.
func NewHandler(backend string, timeout time.Duration, logger *slog.Logger) (http.Handler, error) {
	var backends []string
	if backend != "" {
		backends = []string{backend}
	}
	return NewPoolHandler(PoolOptions{Backends: backends, Policy: "least-inflight", MaxInflight: 1}, timeout, logger)
}

// NewPoolHandler routes each request once and holds capacity until forwarding ends.
func NewPoolHandler(options PoolOptions, timeout time.Duration, logger *slog.Logger) (*Handler, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("request timeout must be positive")
	}
	pool := &workerPool{limit: options.MaxInflight, policy: options.Policy}
	seen := make(map[string]bool)
	for _, backend := range options.Backends {
		target, err := url.Parse(backend)
		if err != nil || target == nil || (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" || target.User != nil || (target.Path != "" && target.Path != "/") || target.RawQuery != "" || target.ForceQuery || target.Fragment != "" {
			return nil, fmt.Errorf("backend must be an http(s) origin without credentials, path, query, or fragment")
		}
		target.Path = ""
		key := strings.ToLower(target.String())
		if seen[key] {
			return nil, fmt.Errorf("duplicate backend origin")
		}
		seen[key] = true
		proxy := &httputil.ReverseProxy{
			Rewrite:       func(p *httputil.ProxyRequest) { p.SetURL(target) },
			FlushInterval: -1,
			ErrorLog:      slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				if errors.Is(r.Context().Err(), context.Canceled) {
					return
				}
				if errors.Is(r.Context().Err(), context.DeadlineExceeded) {
					http.Error(w, "worker deadline exceeded", http.StatusGatewayTimeout)
					return
				}
				logger.Warn("worker request failed", "error", err)
				http.Error(w, "worker unavailable", http.StatusBadGateway)
			},
		}
		pool.proxies = append(pool.proxies, proxy)
		pool.inflight = append(pool.inflight, 0)
		pool.states = append(pool.states, WorkerState{ID: len(pool.states), Origin: target.String(), Healthy: true})
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{\"status\":\"ok\"}\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		for _, state := range pool.states {
			if state.Healthy && !state.Draining {
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		http.Error(w, "no eligible workers", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if len(pool.proxies) == 0 {
			http.Error(w, "no worker configured", http.StatusServiceUnavailable)
			return
		}
		prefixKey := r.Header.Get("X-Prefix-Key")
		if len(prefixKey) > 256 {
			http.Error(w, "prefix key exceeds 256 bytes", http.StatusBadRequest)
			return
		}
		index, available := pool.acquire(prefixKey)
		if !available {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "no healthy undrained worker with capacity", http.StatusServiceUnavailable)
			return
		}
		defer pool.release(index)
		logger.Debug("worker selected", "worker", index, "policy", options.Policy)
		// Buffer only the bounded request; the response is copied incrementally.
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "request exceeds 64 KiB", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "cannot read request", http.StatusBadRequest)
			return
		}
		if !json.Valid(body) {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		started := time.Now()
		defer func() {
			logger.Info("forwarding ended", "worker", index, "duration", time.Since(started), "context_error", ctx.Err())
		}()
		// Bound downstream writes too: context cancellation alone cannot interrupt
		// a blocked network write to a client that has stopped reading.
		controller := http.NewResponseController(w)
		// One extra second permits a pre-stream 504 to reach the client when the
		// upstream deadline expires. This is still a fixed bound on blocked writes.
		if err := controller.SetWriteDeadline(time.Now().Add(timeout + time.Second)); err != nil {
			logger.Error("cannot set stream write deadline", "error", err)
			http.Error(w, "streaming unavailable", http.StatusInternalServerError)
			return
		}
		defer controller.SetWriteDeadline(time.Time{})
		r = r.WithContext(ctx)
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		r.TransferEncoding = nil
		r.Header.Del("X-Prefix-Key") // Routing metadata is not part of the engine request.
		pool.proxies[index].ServeHTTP(w, r)
	})
	return &Handler{Handler: mux, pool: pool}, nil
}
