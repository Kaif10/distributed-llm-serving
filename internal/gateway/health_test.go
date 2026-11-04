package gateway

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func monitored(t *testing.T, origins []string) *Handler {
	t.Helper()
	h, err := NewPoolHandler(PoolOptions{Backends: origins, Policy: "round-robin", MaxInflight: 1}, 5*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	stop, err := h.StartHealthChecks(context.Background(), 20*time.Millisecond, 100*time.Millisecond, "/health")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	return h
}

func TestHealthFailureRecoveryAndReadiness(t *testing.T) {
	var status atomic.Int32
	status.Store(503)
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(int(status.Load()))
			return
		}
		io.WriteString(w, "generated")
	}))
	defer worker.Close()
	h := monitored(t, []string{worker.URL})
	server := httptest.NewServer(h)
	defer server.Close()
	if post(t, server, payload).StatusCode != 503 {
		t.Fatal("unknown/unhealthy worker selected")
	}
	status.Store(200)
	eventually(t, func() bool { return h.snapshot()[0].Healthy })
	response := post(t, server, payload)
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("recovered worker not selected")
	}
	status.Store(503)
	eventually(t, func() bool { return !h.snapshot()[0].Healthy })
	if post(t, server, payload).StatusCode != 503 {
		t.Fatal("unhealthy worker selected")
	}
	for _, path := range []string{"/readyz", "/healthz"} {
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		want := 200
		if path == "/readyz" {
			want = 503
		}
		if response.StatusCode != want {
			t.Fatalf("%s: %d", path, response.StatusCode)
		}
	}
	status.Store(200)
	eventually(t, func() bool { return h.snapshot()[0].Healthy })
}

func TestDrainPreservesStreamAndHealthDoesNotResumeIt(t *testing.T) {
	finish := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(finish) }) }
	var origins []string
	var servers []*httptest.Server
	for i := 0; i < 2; i++ {
		worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(200)
				return
			}
			io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "text/event-stream")
			if i == 0 {
				io.WriteString(w, "data: A\n\n")
				w.(http.Flusher).Flush()
				select {
				case <-finish:
				case <-r.Context().Done():
					return
				}
			} else {
				io.WriteString(w, "data: B\n\n")
			}
			io.WriteString(w, "data: [DONE]\n\n")
		}))
		servers = append(servers, worker)
		origins = append(origins, worker.URL)
	}
	defer func() {
		release()
		for _, server := range servers {
			server.Close()
		}
	}()
	h := monitored(t, origins)
	server := httptest.NewServer(h)
	defer server.Close()
	admin := httptest.NewServer(h.AdminHandler())
	defer admin.Close()
	eventually(t, func() bool { s := h.snapshot(); return s[0].Healthy && s[1].Healthy })
	first := post(t, server, payload)
	defer first.Body.Close()
	reader := bufio.NewReader(first.Body)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(line, "A") {
		t.Fatal("first worker did not stream")
	}
	drain, err := admin.Client().Post(admin.URL+"/workers/0/drain", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	drain.Body.Close()
	s := h.snapshot()[0]
	if !s.Draining || s.Drained || s.Inflight != 1 {
		t.Fatalf("wrong drain state %+v", s)
	}
	checked := s.CheckedAt
	eventually(t, func() bool { return h.snapshot()[0].CheckedAt.After(checked) })
	if !h.snapshot()[0].Draining {
		t.Fatal("health probe undid drain")
	}
	second := post(t, server, payload)
	body, err := io.ReadAll(second.Body)
	second.Body.Close()
	if err != nil || !strings.Contains(string(body), "data: B") {
		t.Fatal("new request sent to draining worker")
	}
	release()
	rest, err := io.ReadAll(reader)
	if err != nil || !strings.Contains(string(rest), "[DONE]") {
		t.Fatal("drain interrupted active stream")
	}
	eventually(t, func() bool { return h.snapshot()[0].Drained })
	resume, err := admin.Client().Post(admin.URL+"/workers/0/resume", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resume.Body.Close()
	if h.snapshot()[0].Draining {
		t.Fatal("resume did not clear drain")
	}
	if response, err := server.Client().Get(server.URL + "/workers"); err != nil {
		t.Fatal(err)
	} else {
		response.Body.Close()
		if response.StatusCode != 404 {
			t.Fatal("admin exposed on data listener")
		}
	}
}

func TestHealthTimeoutAndStop(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer worker.Close()
	h, _ := NewPoolHandler(PoolOptions{Backends: []string{worker.URL}, Policy: "round-robin", MaxInflight: 1}, time.Second, slog.Default())
	stop, err := h.StartHealthChecks(context.Background(), time.Millisecond, 50*time.Millisecond, "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	eventually(t, func() bool { return !h.snapshot()[0].CheckedAt.IsZero() })
	if h.snapshot()[0].Healthy {
		t.Fatal("timed-out probe became healthy")
	}
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe shutdown blocked")
	}
}
