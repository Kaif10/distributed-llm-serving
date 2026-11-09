package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/control"
)

type ControllerOptions struct {
	Origin                    string
	Origins                   []string
	Interval, Timeout, MaxAge time.Duration
}
type ConfigStatus struct {
	Managed          bool      `json:"managed"`
	Applied          bool      `json:"applied"`
	Revision         uint64    `json:"applied_revision"`
	Fresh            bool      `json:"fresh"`
	ConfirmedAt      time.Time `json:"confirmed_at"`
	ExpiresAt        time.Time `json:"expires_at"`
	LastError        string    `json:"last_error,omitempty"`
	ControllerOrigin string    `json:"controller_origin,omitempty"`
}

func (p *workerPool) freshLocked() bool {
	return !p.config.Managed || (p.config.Applied && time.Now().Before(p.config.ExpiresAt))
}

// This increment applies desired drains to an existing fixed-membership pool.
// Unsupported membership/origin changes are rejected without refreshing its view.
func (h *Handler) applyConfig(snapshot control.Snapshot, started time.Time, maxAge time.Duration) error {
	return h.applyConfigFrom(snapshot, started, maxAge, "")
}

func (h *Handler) applyConfigFrom(snapshot control.Snapshot, started time.Time, maxAge time.Duration, origin string) error {
	if err := control.ValidateWorkers(snapshot.Workers); err != nil {
		return err
	}
	encoded, _ := json.Marshal(snapshot)
	p := h.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(started) >= maxAge {
		return fmt.Errorf("snapshot response already expired")
	}
	if p.config.Applied && (snapshot.Revision < p.config.Revision || (snapshot.Revision == p.config.Revision && string(encoded) != p.configBytes)) {
		return fmt.Errorf("rollback or conflicting snapshot revision")
	}
	if len(snapshot.Workers) != len(p.states) {
		return fmt.Errorf("snapshot must match configured worker membership")
	}
	byOrigin := make(map[string]control.Worker)
	for _, w := range snapshot.Workers {
		byOrigin[w.Origin] = w
	}
	for _, state := range p.states {
		if _, ok := byOrigin[state.Origin]; !ok {
			return fmt.Errorf("snapshot origin does not match configured worker")
		}
	}
	for i := range p.states {
		w := byOrigin[p.states[i].Origin]
		p.states[i].Draining = w.Draining
		p.states[i].ConfigID = w.ID
	}
	p.config.Applied = true
	p.config.Revision = snapshot.Revision
	p.config.ConfirmedAt = time.Now().UTC()
	p.config.ExpiresAt = started.Add(maxAge)
	p.config.LastError = ""
	p.config.ControllerOrigin = origin
	p.configBytes = string(encoded)
	return nil
}

// controllerOrigins preserves the single-controller option while requiring one
// explicit, bounded list for replicated reads. The list must name one group.
func controllerOrigins(options ControllerOptions) ([]string, error) {
	if options.Origin != "" && len(options.Origins) > 0 {
		return nil, fmt.Errorf("use one controller origin or a peer list")
	}
	origins := append([]string{}, options.Origins...)
	if options.Origin != "" {
		origins = []string{options.Origin}
	}
	if len(origins) < 1 || len(origins) > 9 || options.Interval <= 0 || options.Timeout <= 0 || options.MaxAge <= options.Timeout {
		return nil, fmt.Errorf("require 1..9 controller origins, positive poll/timeout, max-age greater than timeout")
	}
	seen := map[string]bool{}
	for _, origin := range origins {
		u, err := url.Parse(origin)
		if err != nil || u == nil || len(origin) > 2048 || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || seen[origin] {
			return nil, fmt.Errorf("invalid or duplicate controller origin")
		}
		seen[origin] = true
	}
	return origins, nil
}

func (h *Handler) fetchConfig(ctx context.Context, client *http.Client, origin string, maxAge time.Duration) error {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", origin+"/config", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Cache-Control", "no-cache")
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("controller HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("snapshot exceeds one MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var snapshot control.Snapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected one snapshot object")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return h.applyConfigFrom(snapshot, started, maxAge, origin)
}

func (h *Handler) StartController(parent context.Context, options ControllerOptions) (func(), error) {
	origins, err := controllerOrigins(options)
	if err != nil {
		return nil, err
	}
	p := h.pool
	p.mu.Lock()
	if p.config.Managed {
		p.mu.Unlock()
		return nil, fmt.Errorf("controller already configured")
	}
	p.config.Managed = true
	p.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 1
	transport.MaxIdleConns = len(origins)
	transport.MaxIdleConnsPerHost = 1
	transport.MaxResponseHeaderBytes = 16 << 10
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		defer client.CloseIdleConnections()
		preferred := 0
		for {
			cycle, stopCycle := context.WithTimeout(ctx, options.Timeout)
			deadline, _ := cycle.Deadline()
			var failures []string
			found := false
			for offset := 0; offset < len(origins); offset++ {
				if cycle.Err() != nil {
					break
				}
				index := (preferred + offset) % len(origins)
				// Share the remaining cycle budget so a live isolated former leader cannot
				// monopolize all attempts. Fast failures leave more time for later peers.
				budget := time.Until(deadline) / time.Duration(len(origins)-offset)
				attempt, stopAttempt := context.WithTimeout(cycle, budget)
				err := h.fetchConfig(attempt, client, origins[index], options.MaxAge)
				stopAttempt()
				if err == nil {
					preferred = index
					found = true
					break
				}
				failures = append(failures, origins[index]+": "+err.Error())
			}
			stopCycle()
			if !found {
				preferred = (preferred + 1) % len(origins)
				p.mu.Lock()
				p.config.LastError = "no fresh controller snapshot: " + strings.Join(failures, "; ")
				p.mu.Unlock()
			}
			timer := time.NewTimer(options.Interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return func() { cancel(); worker.Wait() }, nil
}
