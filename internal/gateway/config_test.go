package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Kaif10/inference-fleet/internal/control"
)

func TestConfigRejectsRollbackConflictAndMembershipWithoutRefresh(t *testing.T) {
	h, err := NewPoolHandler(PoolOptions{Backends: []string{"http://127.0.0.1:1"}, Policy: "least-inflight", MaxInflight: 1}, time.Second, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	h.pool.config.Managed = true
	snapshot := control.Snapshot{Revision: 2, Workers: []control.Worker{{ID: "a", Origin: "http://127.0.0.1:1", Draining: true}}}
	if err := h.applyConfig(snapshot, time.Now().Add(-100*time.Millisecond), time.Second); err != nil {
		t.Fatal(err)
	}
	expires := h.pool.config.ExpiresAt
	bad := []control.Snapshot{{Revision: 1, Workers: snapshot.Workers}, {Revision: 2, Workers: []control.Worker{{ID: "a", Origin: "http://127.0.0.1:1"}}}, {Revision: 3, Workers: []control.Worker{{ID: "a", Origin: "http://127.0.0.1:2"}}}}
	for _, s := range bad {
		if h.applyConfig(s, time.Now(), time.Second) == nil {
			t.Fatal("invalid replacement accepted")
		}
		if h.pool.config.ExpiresAt != expires || !h.snapshot()[0].Draining {
			t.Fatal("rejected view changed state/freshness")
		}
	}
	if err := h.applyConfig(snapshot, time.Now(), time.Second); err != nil {
		t.Fatal("same-revision confirmation rejected", err)
	}
	if !h.pool.config.ExpiresAt.After(expires) {
		t.Fatal("valid confirmation did not refresh view")
	}
}

func TestControllerDrainExpiryAndRecoveryPreserveActiveStream(t *testing.T) {
	finish := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(finish) }) }
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-finish:
			io.WriteString(w, "data: [DONE]\n\n")
		case <-r.Context().Done():
		}
	}))
	defer worker.Close()
	defer release()
	var mu sync.Mutex
	available := false
	snapshot := control.Snapshot{Revision: 1, Workers: []control.Worker{{ID: "stable-a", Origin: worker.URL}}}
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if !available {
			http.Error(w, "unavailable", 503)
			return
		}
		json.NewEncoder(w).Encode(snapshot)
	}))
	defer controller.Close()
	h, err := NewPoolHandler(PoolOptions{Backends: []string{worker.URL}, Policy: "least-inflight", MaxInflight: 1}, 3*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stop, err := h.StartController(context.Background(), ControllerOptions{Origin: controller.URL, Interval: 10 * time.Millisecond, Timeout: 50 * time.Millisecond, MaxAge: 250 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	server := httptest.NewServer(h)
	defer server.Close()
	admin := httptest.NewServer(h.AdminHandler())
	defer admin.Close()
	if post(t, server, payload).StatusCode != 503 {
		t.Fatal("served before first config")
	}
	mu.Lock()
	available = true
	mu.Unlock()
	eventually(t, func() bool { h.pool.mu.Lock(); defer h.pool.mu.Unlock(); return h.pool.config.Applied })
	first := post(t, server, payload)
	defer first.Body.Close()
	reader := bufio.NewReader(first.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	snapshot.Revision = 2
	snapshot.Workers[0].Draining = true
	mu.Unlock()
	eventually(t, func() bool { return h.snapshot()[0].Draining })
	if h.snapshot()[0].ConfigID != "stable-a" {
		t.Fatal("logical ID not exposed")
	}
	local, err := admin.Client().Post(admin.URL+"/workers/0/resume", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	local.Body.Close()
	if local.StatusCode != 409 {
		t.Fatal("local admin overrode controller")
	}
	mu.Lock()
	available = false
	mu.Unlock()
	eventually(t, func() bool { h.pool.mu.Lock(); defer h.pool.mu.Unlock(); return !h.pool.freshLocked() })
	if post(t, server, payload).StatusCode != 503 {
		t.Fatal("expired configuration served new work")
	}
	release()
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), "[DONE]") {
		t.Fatal("config update/expiry interrupted active stream", err)
	}
	mu.Lock()
	snapshot.Revision = 3
	snapshot.Workers[0].Draining = false
	available = true
	mu.Unlock()
	eventually(t, func() bool {
		h.pool.mu.Lock()
		defer h.pool.mu.Unlock()
		return h.pool.config.Revision == 3 && h.pool.freshLocked() && h.pool.inflight[0] == 0
	})
	response := post(t, server, payload)
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, response.Body)
	if err != nil || response.StatusCode != 200 {
		t.Fatal("recovery failed", err)
	}
}
