package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Kaif10/inference-fleet/internal/control"
)

type ControllerOptions struct {
	Origin                    string
	Interval, Timeout, MaxAge time.Duration
}
type ConfigStatus struct {
	Managed     bool      `json:"managed"`
	Applied     bool      `json:"applied"`
	Revision    uint64    `json:"applied_revision"`
	Fresh       bool      `json:"fresh"`
	ConfirmedAt time.Time `json:"confirmed_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	LastError   string    `json:"last_error,omitempty"`
}

func (p *workerPool) freshLocked() bool {
	return !p.config.Managed || (p.config.Applied && time.Now().Before(p.config.ExpiresAt))
}

// This increment applies desired drains to an existing fixed-membership pool.
// Unsupported membership/origin changes are rejected without refreshing its view.
func (h *Handler) applyConfig(snapshot control.Snapshot, started time.Time, maxAge time.Duration) error {
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
	p.configBytes = string(encoded)
	return nil
}

func (h *Handler) StartController(parent context.Context, options ControllerOptions) (func(), error) {
	u, err := url.Parse(options.Origin)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || options.Interval <= 0 || options.Timeout <= 0 || options.MaxAge <= options.Timeout {
		return nil, fmt.Errorf("controller requires http(s) origin, positive poll/timeout, max-age greater than timeout")
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
	client := &http.Client{Timeout: options.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		for {
			started := time.Now()
			err := func() error {
				req, _ := http.NewRequestWithContext(ctx, "GET", options.Origin+"/config", nil)
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
				return h.applyConfig(snapshot, started, options.MaxAge)
			}()
			if err != nil {
				p.mu.Lock()
				p.config.LastError = err.Error()
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
