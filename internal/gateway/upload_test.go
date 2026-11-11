package gateway

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type observedUpload struct {
	io.ReadCloser
	once    sync.Once
	reading chan struct{}
}

func (b *observedUpload) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	return b.ReadCloser.Read(p)
}

func TestSlowUploadDoesNotReserveInferenceCapacity(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, "generated")
	}))
	defer worker.Close()
	h, err := NewPoolHandler(PoolOptions{Backends: []string{worker.URL}, Policy: "least-inflight", MaxInflight: 1}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	reading := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Slow-Upload") == "yes" {
			r.Body = &observedUpload{ReadCloser: r.Body, reading: reading}
		}
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: test\r\nX-Slow-Upload: yes\r\nContent-Length: %d\r\n\r\n{", len(payload))
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("body read did not start")
	}
	if h.snapshot()[0].Inflight != 0 {
		t.Fatal("unfinished upload occupies inference slot")
	}
	response := post(t, server, payload)
	if response.StatusCode != 200 {
		t.Fatalf("complete request rejected while engine idle and another client uploads: HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "generated" {
		t.Fatal(string(body), err)
	}
}

func TestUploadLimitDeadlineAndRecovery(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, "generated")
	}))
	defer worker.Close()
	h, err := NewPoolHandler(PoolOptions{Backends: []string{worker.URL}, Policy: "least-inflight", MaxInflight: 1, MaxUploads: 1, UploadTimeout: 500 * time.Millisecond}, time.Second, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	reading := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Slow") == "yes" {
			r.Body = &observedUpload{ReadCloser: r.Body, reading: reading}
		}
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: test\r\nX-Slow: yes\r\nContent-Length: %d\r\n\r\n{", len(payload))
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	rejected := post(t, server, payload)
	body, _ := io.ReadAll(rejected.Body)
	rejected.Body.Close()
	if rejected.StatusCode != 503 || rejected.Header.Get("Retry-After") != "1" || !strings.Contains(string(body), "upload capacity") {
		t.Fatal(rejected.StatusCode, string(body))
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 408 {
		t.Fatal("stalled upload not timed out", response.StatusCode)
	}
	if h.snapshot()[0].Inflight != 0 {
		t.Fatal("upload timeout leaked worker reservation")
	}
	if response := post(t, server, payload); response.StatusCode != 200 {
		t.Fatal("upload admission not released after timeout", response.StatusCode)
	}
}

func TestDrainDuringUploadIsCheckedBeforeDispatch(t *testing.T) {
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer worker.Close()
	h, err := NewPoolHandler(PoolOptions{Backends: []string{worker.URL}, Policy: "least-inflight", MaxInflight: 1}, time.Second, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	reading := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &observedUpload{ReadCloser: r.Body, reading: reading}
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n{", len(payload))
	select {
	case <-reading:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	h.pool.mu.Lock()
	h.pool.states[0].Draining = true
	h.pool.mu.Unlock()
	if _, err := io.WriteString(conn, payload[1:]); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 503 || calls.Load() != 0 {
		t.Fatal("pending upload bypassed drain", response.StatusCode, calls.Load())
	}
}

func TestTwoGatewaysRespectWorkerAdmissionBoundary(t *testing.T) {
	busy := make(chan struct{}, 1)
	released := make(chan struct{}, 2)
	var rejected atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case busy <- struct{}{}:
		default:
			rejected.Add(1)
			http.Error(w, "engine capacity full", 503)
			return
		}
		defer func() { <-busy; released <- struct{}{} }()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer worker.Close()
	a := startGateway(t, worker.URL, 5*time.Second)
	b := startGateway(t, worker.URL, 5*time.Second)
	first := post(t, a, payload)
	defer first.Body.Close()
	if first.StatusCode != 200 {
		t.Fatal(first.StatusCode)
	}
	second := post(t, b, payload)
	defer second.Body.Close()
	if second.StatusCode != 503 || rejected.Load() != 1 {
		t.Fatal("worker admission did not bound independent gateways", second.StatusCode, rejected.Load())
	}
	first.Body.Close()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("cancelled worker not released")
	}
	third := post(t, b, payload)
	defer third.Body.Close()
	if third.StatusCode != 200 {
		t.Fatal("second gateway could not use released worker", third.StatusCode)
	}
}

func TestUploadBudgetReleasedDuringGeneration(t *testing.T) {
	var origins []string
	for i := 0; i < 2; i++ {
		worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		origins = append(origins, worker.URL)
		defer worker.Close()
	}
	h, err := NewPoolHandler(PoolOptions{Backends: origins, Policy: "least-inflight", MaxInflight: 1, MaxUploads: 1}, time.Second, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	first := post(t, server, payload)
	defer first.Body.Close()
	second := post(t, server, payload)
	defer second.Body.Close()
	if first.StatusCode != 200 || second.StatusCode != 200 {
		t.Fatal("one upload token serialized inference", first.StatusCode, second.StatusCode)
	}
	third := post(t, server, payload)
	defer third.Body.Close()
	body, _ := io.ReadAll(third.Body)
	if third.StatusCode != 503 || strings.Contains(string(body), "upload capacity") {
		t.Fatal("serving capacity was not enforced separately", third.StatusCode, string(body))
	}
}
