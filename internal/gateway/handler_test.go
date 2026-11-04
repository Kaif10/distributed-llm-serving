package gateway

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kaif10/inference-fleet/internal/testworker"
)

const payload = `{"model":"test-worker","messages":[{"role":"user","content":"hello"}],"stream":true,"max_tokens":3}`

func startGateway(t *testing.T, backend string, timeout time.Duration) *httptest.Server {
	t.Helper()
	handler, err := NewHandler(backend, timeout, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func post(t *testing.T, server *httptest.Server, body string) *http.Response {
	t.Helper()
	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func TestStreamAndCancellationCrossGateway(t *testing.T) {
	finished := make(chan struct{})
	workerHandler := testworker.NewHandler(time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		workerHandler.ServeHTTP(w, r)
	}))
	defer worker.Close()
	gateway := startGateway(t, worker.URL, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", gateway.URL+"/v1/chat/completions", strings.NewReader(payload))
	response, err := gateway.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || !strings.Contains(line, "test-chunk-1") {
		t.Fatalf("first chunk: %q, %v", line, err)
	}
	select {
	case <-finished:
		t.Fatal("worker finished too early")
	default:
	}
	cancel()
	response.Body.Close()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect did not cancel worker through gateway")
	}
}

func TestCompletedStreamMatchesDirectWorker(t *testing.T) {
	worker := httptest.NewServer(testworker.NewHandler(time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer worker.Close()
	gateway := startGateway(t, worker.URL, 3*time.Second)
	direct := post(t, worker, payload)
	forwarded := post(t, gateway, payload)
	a, err := io.ReadAll(direct.Body)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(forwarded.Body)
	if err != nil {
		t.Fatal(err)
	}
	if forwarded.StatusCode != 200 || forwarded.Header.Get("Content-Type") != "text/event-stream" || string(a) != string(b) || !strings.Contains(string(b), "[DONE]") {
		t.Fatalf("forwarded response differs: %d %q", forwarded.StatusCode, b)
	}
}

func TestWorkerStatusIsPreservedAndNoRetry(t *testing.T) {
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "2")
		http.Error(w, "worker is busy", http.StatusTooManyRequests)
	}))
	defer worker.Close()
	response := post(t, startGateway(t, worker.URL, time.Second), payload)
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 429 || response.Header.Get("Retry-After") != "2" || string(body) != "worker is busy\n" || calls.Load() != 1 {
		t.Fatal("worker rejection was changed or retried")
	}
}

func TestPreStreamFailures(t *testing.T) {
	t.Run("no backend", func(t *testing.T) {
		if post(t, startGateway(t, "", time.Second), payload).StatusCode != 503 {
			t.Fatal("expected 503")
		}
	})
	t.Run("unreachable backend", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		listener.Close()
		if post(t, startGateway(t, "http://"+address, time.Second), payload).StatusCode != 502 {
			t.Fatal("expected 502")
		}
	})
	t.Run("deadline", func(t *testing.T) {
		finished := make(chan struct{})
		worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Consume the body so the server can detect the peer closing its connection.
			io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			close(finished)
		}))
		defer worker.Close()
		if post(t, startGateway(t, worker.URL, 100*time.Millisecond), payload).StatusCode != 504 {
			t.Fatal("expected 504")
		}
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatal("deadline did not cancel worker")
		}
	})
}

func TestTruncatedWorkerStreamIsNotSuccessful(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: partial\n\n")
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // Simulate a worker dying after sending headers.
	}))
	defer worker.Close()
	response := post(t, startGateway(t, worker.URL, time.Second), payload)
	body, err := io.ReadAll(response.Body)
	if err == nil || !strings.Contains(string(body), "partial") || strings.Contains(string(body), "[DONE]") {
		t.Fatalf("truncation hidden: %q %v", body, err)
	}
}

func TestInvalidBodyNeverReachesWorker(t *testing.T) {
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer worker.Close()
	gateway := startGateway(t, worker.URL, time.Second)
	for _, tc := range []struct {
		body   string
		status int
	}{{"{", 400}, {strings.Repeat("x", maxRequestBytes+1), 413}} {
		if response := post(t, gateway, tc.body); response.StatusCode != tc.status {
			t.Fatalf("got %d, want %d", response.StatusCode, tc.status)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input dispatched")
	}
}

func TestNonReadingClientCannotHoldGatewayForever(t *testing.T) {
	workerDone := make(chan struct{})
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(workerDone)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		chunk := strings.Repeat("x", 32<<10)
		for {
			if r.Context().Err() != nil {
				return
			}
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
		}
	}))
	defer worker.Close()
	handler, err := NewHandler(worker.URL, 200*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetReadBuffer(1024)
	}
	// Send a complete request and never read the response. The worker emits
	// enough data to fill socket buffers and block the gateway's downstream write.
	request, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", strings.NewReader(payload))
	if err := request.Write(conn); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(4 * time.Second):
		t.Fatal("gateway stuck writing to non-reading client")
	}
	select {
	case <-workerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("worker not released after downstream timeout")
	}
}

func TestRejectInvalidConfiguration(t *testing.T) {
	for _, backend := range []string{"ftp://localhost", "http://", "http://user:pass@localhost", "http://localhost/v1", "http://localhost?x=y", "http://localhost/#fragment"} {
		if _, err := NewHandler(backend, time.Second, slog.Default()); err == nil {
			t.Fatalf("accepted %q", backend)
		}
	}
	if _, err := NewHandler("", 0, slog.Default()); err == nil {
		t.Fatal("accepted zero timeout")
	}
}
