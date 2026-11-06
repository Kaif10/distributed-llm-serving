package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Handler struct {
	http.Handler
	pool *workerPool
}

// StartHealthChecks starts one sequential probe loop per worker. Workers are
// ineligible until their first successful probe. The returned function joins loops.
// Call once, before accepting traffic. Probe results never modify drain state.
func (h *Handler) StartHealthChecks(parent context.Context, interval, timeout time.Duration, path string) (func(), error) {
	u, err := url.ParseRequestURI(path)
	if interval <= 0 || timeout <= 0 || err != nil || u == nil || u.Host != "" || u.Scheme != "" || u.Path == "" || u.Path[0] != '/' || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("health interval/timeout must be positive and path must be an absolute path without a query")
	}
	ctx, cancel := context.WithCancel(parent)
	client := &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	h.pool.mu.Lock()
	for i := range h.pool.states {
		h.pool.states[i].Healthy = false
	}
	h.pool.mu.Unlock()
	var workers sync.WaitGroup
	for i := range h.pool.states {
		origin := h.pool.states[i].Origin
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				req, _ := http.NewRequestWithContext(ctx, "GET", origin+path, nil)
				response, err := client.Do(req)
				healthy := err == nil && response.StatusCode == http.StatusOK
				if response != nil {
					response.Body.Close()
				}
				h.pool.mu.Lock()
				h.pool.states[i].Healthy = healthy && ctx.Err() == nil
				h.pool.states[i].CheckedAt = time.Now().UTC()
				h.pool.mu.Unlock()
				timer := time.NewTimer(interval)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	}
	return func() { cancel(); workers.Wait() }, nil
}

func (h *Handler) snapshot() []WorkerState {
	h.pool.mu.Lock()
	defer h.pool.mu.Unlock()
	states := append([]WorkerState{}, h.pool.states...)
	for i := range states {
		states[i].Inflight = h.pool.inflight[i]
		states[i].Drained = states[i].Draining && states[i].Inflight == 0
	}
	return states
}

// AdminHandler belongs on the separate loopback-only listener, never on the data API.
func (h *Handler) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) {
		h.pool.mu.Lock()
		status := h.pool.config
		status.Fresh = h.pool.freshLocked()
		h.pool.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("GET /workers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(h.snapshot())
	})
	set := func(draining bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			id, err := strconv.Atoi(r.PathValue("id"))
			if err != nil || id < 0 || id >= len(h.pool.proxies) {
				http.NotFound(w, r)
				return
			}
			h.pool.mu.Lock()
			if h.pool.config.Managed {
				h.pool.mu.Unlock()
				http.Error(w, "update drain state through the controller", http.StatusConflict)
				return
			}
			h.pool.states[id].Draining = draining
			h.pool.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(h.snapshot()[id])
		}
	}
	mux.HandleFunc("POST /workers/{id}/drain", set(true))
	mux.HandleFunc("POST /workers/{id}/resume", set(false))
	return mux
}
