package gateway

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoliciesUseCapacityAndLoad(t *testing.T) {
	for _, policy := range []string{"round-robin", "least-inflight"} {
		p := &workerPool{proxies: make([]*httputil.ReverseProxy, 3), inflight: make([]int, 3), limit: 3, policy: policy}
		for _, want := range []int{0, 1, 2, 0} {
			if got, ok := p.acquire(); !ok || got != want {
				t.Fatalf("%s: got %d, want %d", policy, got, want)
			}
		}
		p.release(2)
		want := 1
		if policy == "least-inflight" {
			want = 2
		}
		if got, _ := p.acquire(); got != want {
			t.Fatalf("%s selected %d, want %d", policy, got, want)
		}
	}
}

func TestPoolConfiguration(t *testing.T) {
	for _, options := range []PoolOptions{
		{Policy: "unknown", MaxInflight: 1},
		{Policy: "least-inflight", MaxInflight: 0},
		{Backends: []string{"http://localhost:8081", "http://localhost:8081/"}, Policy: "round-robin", MaxInflight: 1},
	} {
		if _, err := NewPoolHandler(options, time.Second, slog.Default()); err == nil {
			t.Fatal("invalid pool configuration accepted")
		}
	}
}

func TestConcurrentAdmissionDoesNotOversubscribe(t *testing.T) {
	p := &workerPool{proxies: make([]*httputil.ReverseProxy, 2), inflight: make([]int, 2), limit: 2, policy: "least-inflight"}
	var attempted, finished sync.WaitGroup
	var accepted atomic.Int32
	release := make(chan struct{})
	for i := 0; i < 64; i++ {
		attempted.Add(1)
		finished.Add(1)
		go func() {
			defer finished.Done()
			index, ok := p.acquire()
			if ok {
				accepted.Add(1)
			}
			attempted.Done()
			if ok {
				<-release
				p.release(index)
			}
		}()
	}
	attempted.Wait()
	if accepted.Load() != 4 {
		t.Errorf("admitted %d, capacity is 4", accepted.Load())
	}
	close(release)
	finished.Wait()
	for i := 0; i < 4; i++ {
		if _, ok := p.acquire(); !ok {
			t.Fatal("capacity leaked after release")
		}
	}
}

func TestPoolRejectsOverloadAndReusesCancelledCapacity(t *testing.T) {
	var calls [2]atomic.Int32
	var workers []*httptest.Server
	for i := 0; i < 2; i++ {
		worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls[i].Add(1)
			io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: waiting\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		workers = append(workers, worker)
		defer worker.Close()
	}
	handler, err := NewPoolHandler(PoolOptions{Backends: []string{workers[0].URL, workers[1].URL}, Policy: "least-inflight", MaxInflight: 1}, 10*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	first := post(t, server, payload)
	defer first.Body.Close()
	second := post(t, server, payload)
	defer second.Body.Close()
	third := post(t, server, payload)
	if third.StatusCode != 503 || third.Header.Get("Retry-After") != "1" {
		t.Fatal("expected bounded overload rejection")
	}
	third.Body.Close()
	if calls[0].Load() != 1 || calls[1].Load() != 1 {
		t.Fatal("requests did not reach distinct workers")
	}
	first.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		replacement := post(t, server, payload)
		replacement.Body.Close()
		if replacement.StatusCode == 200 {
			break
		}
		if replacement.StatusCode != 503 || time.Now().After(deadline) {
			t.Fatal("cancelled stream leaked capacity")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls[0].Load() != 2 || calls[1].Load() != 1 {
		t.Fatal("replacement was not routed to released worker")
	}
}

func TestFailureAndInvalidRequestReleaseCapacity(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic(http.ErrAbortHandler) }))
	defer worker.Close()
	server := startGateway(t, worker.URL, time.Second)
	for i := 0; i < 2; i++ {
		bad := post(t, server, "{")
		bad.Body.Close()
		if bad.StatusCode != 400 {
			t.Fatal("validation leaked capacity")
		}
		failed := post(t, server, payload)
		failed.Body.Close()
		if failed.StatusCode != 502 {
			t.Fatal("upstream failure leaked capacity")
		}
	}
}
