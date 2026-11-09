package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/control"
)

func TestControllerPeerFallbackPreferenceExpiryAndRecovery(t *testing.T) {
	var hangs, thirdReads atomic.Int64
	var secondReady, thirdReady atomic.Bool
	thirdReady.Store(true)
	hanging := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hangs.Add(1); <-r.Context().Done() }))
	defer hanging.Close()
	worker := "http://127.0.0.1:1"
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !secondReady.Load() {
			http.Error(w, "follower", 503)
			return
		}
		json.NewEncoder(w).Encode(control.Snapshot{Revision: 2, Workers: []control.Worker{{ID: "worker", Origin: worker, Draining: true}}})
	}))
	defer second.Close()
	third := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		thirdReads.Add(1)
		if !thirdReady.Load() {
			http.Error(w, "former leader", 503)
			return
		}
		json.NewEncoder(w).Encode(control.Snapshot{Revision: 1, Workers: []control.Worker{{ID: "worker", Origin: worker}}})
	}))
	defer third.Close()
	h, err := NewPoolHandler(PoolOptions{Backends: []string{worker}, Policy: "least-inflight", MaxInflight: 1}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stop, err := h.StartController(context.Background(), ControllerOptions{Origins: []string{hanging.URL, second.URL, third.URL}, Interval: 10 * time.Millisecond, Timeout: 240 * time.Millisecond, MaxAge: 600 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	status := func() ConfigStatus {
		h.pool.mu.Lock()
		defer h.pool.mu.Unlock()
		s := h.pool.config
		s.Fresh = h.pool.freshLocked()
		return s
	}
	eventually(t, func() bool { return status().ControllerOrigin == third.URL && thirdReads.Load() >= 3 })
	if hangs.Load() != 1 {
		t.Fatalf("did not prefer successful peer: slow attempts=%d", hangs.Load())
	}
	secondReady.Store(true)
	thirdReady.Store(false)
	eventually(t, func() bool { s := status(); return s.ControllerOrigin == second.URL && s.Revision == 2 && s.Fresh })
	if !h.snapshot()[0].Draining || status().LastError != "" {
		t.Fatal("new leader configuration was not applied")
	}
	// The old peer returns a syntactically valid rollback. It must not renew the
	// last good view or replace its source, even when every other attempt fails.
	secondReady.Store(false)
	thirdReady.Store(true)
	eventually(t, func() bool { return !status().Fresh })
	expired := status()
	if expired.Revision != 2 || expired.ControllerOrigin != second.URL || expired.LastError == "" {
		t.Fatalf("invalid fallback refreshed state: %+v", expired)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	response := post(t, server, payload)
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("expired configuration admitted work")
	}
	secondReady.Store(true)
	eventually(t, func() bool {
		s := status()
		return s.Fresh && s.Revision == 2 && s.ControllerOrigin == second.URL && s.ExpiresAt.After(expired.ExpiresAt)
	})
}

func TestControllerPeerValidationAndNoRedirectFollowing(t *testing.T) {
	options := ControllerOptions{Origins: []string{"http://127.0.0.1:1"}, Interval: time.Second, Timeout: time.Second, MaxAge: 2 * time.Second}
	for _, origins := range [][]string{nil, {"http://127.0.0.1:1", "http://127.0.0.1:1"}, {"http://user@127.0.0.1:1"}, {"http://127.0.0.1:1/path"}, {"http://127.0.0.1:1?"}, {""}, make([]string, 10)} {
		bad := options
		bad.Origins = origins
		if _, err := controllerOrigins(bad); err == nil {
			t.Fatalf("accepted peers %v", origins)
		}
	}
	bad := options
	bad.Origin = "http://127.0.0.1:2"
	if _, err := controllerOrigins(bad); err == nil {
		t.Fatal("accepted both configuration forms")
	}
	var unexpected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { unexpected.Add(1); w.WriteHeader(500) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/config", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	worker := "http://127.0.0.1:1"
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(control.Snapshot{Revision: 1, Workers: []control.Worker{{ID: "worker", Origin: worker}}})
	}))
	defer good.Close()
	h, err := NewPoolHandler(PoolOptions{Backends: []string{worker}, Policy: "least-inflight", MaxInflight: 1}, time.Second, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	options.Origins = []string{redirect.URL, good.URL}
	options.Interval = 10 * time.Millisecond
	stop, err := h.StartController(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	eventually(t, func() bool {
		h.pool.mu.Lock()
		defer h.pool.mu.Unlock()
		return h.pool.config.ControllerOrigin == good.URL
	})
	if unexpected.Load() != 0 {
		t.Fatal("followed an unconfigured redirect")
	}
}
