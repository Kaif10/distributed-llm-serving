package testworker

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const validRequest = `{"model":"test-worker","messages":[{"role":"user","content":"hello"}],"stream":true,"max_tokens":3}`

func TestStreamArrivesBeforeCompletionAndDisconnectStopsWorker(t *testing.T) {
	finished := make(chan struct{})
	handler := NewHandler(time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader(validRequest))
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || !strings.Contains(line, "test-chunk-1") {
		t.Fatalf("unexpected response: %d %s", response.StatusCode, line)
	}
	select {
	case <-finished:
		t.Fatal("worker finished before generating remaining chunks")
	default:
	}
	// Cancel while the worker waits an hour for its second chunk. Returning promptly
	// proves cancellation interrupts the wait, rather than waiting for another write.
	cancel()
	_ = response.Body.Close()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after disconnect")
	}
}

func TestCompleteStream(t *testing.T) {
	server := httptest.NewServer(NewHandler(time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	response, err := client.Post(server.URL+"/v1/chat/completions", "application/json", strings.NewReader(validRequest))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal("expected SSE response")
	}
	scanner := bufio.NewScanner(response.Body)
	var contents []string
	stopped, done := false, false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if done {
			t.Fatal("event after DONE")
		}
		if line == "data: [DONE]" {
			done = true
			continue
		}
		if !strings.HasPrefix(line, "data: ") {
			t.Fatalf("invalid event %q", line)
		}
		var event struct {
			Choices []struct {
				Delta        struct{ Content string }
				FinishReason *string `json:"finish_reason"`
			}
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if len(event.Choices) != 1 {
			t.Fatal("expected one choice")
		}
		choice := event.Choices[0]
		if choice.FinishReason != nil {
			stopped = *choice.FinishReason == "stop"
			continue
		}
		contents = append(contents, choice.Delta.Content)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(contents, "") != "test-chunk-1 test-chunk-2 test-chunk-3 " || !stopped || !done {
		t.Fatalf("incomplete stream: %v stop=%v done=%v", contents, stopped, done)
	}
}

func TestRejectInvalidRequests(t *testing.T) {
	handler := NewHandler(time.Millisecond, slog.Default())
	for _, body := range []string{
		`{`, validRequest + `{}`, strings.Replace(validRequest, `"max_tokens":3`, `"max_tokens":129`, 1),
		strings.Replace(validRequest, `"stream":true`, `"stream":false`, 1),
		strings.Replace(validRequest, `"hello"`, `"`+strings.Repeat("x", 65<<10)+`"`, 1),
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body)))
		if response.Code != 400 {
			t.Fatalf("expected rejection, got %d", response.Code)
		}
		if strings.Contains(response.Body.String(), "data:") {
			t.Fatal("invalid request started a stream")
		}
	}
}
