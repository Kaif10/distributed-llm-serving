// Package testworker provides synthetic streams for network experiments, not inference.
package testworker

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type request struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	Stream    bool `json:"stream"`
	MaxTokens int  `json:"max_tokens"`
}

// NewHandler emits its first chunk immediately, then waits interval between chunks.
// The tiny chat-completions subset is intentional; generated content ignores the prompt.
func NewHandler(interval time.Duration, logger *slog.Logger) http.Handler {
	if interval <= 0 {
		panic("test worker interval must be positive")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{\"status\":\"ok\"}\n")
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var input request
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid request (maximum 64 KiB)", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "expected one JSON object", http.StatusBadRequest)
			return
		}
		if input.Model != "test-worker" || !input.Stream || len(input.Messages) == 0 || input.MaxTokens < 1 || input.MaxTokens > 128 {
			http.Error(w, "require model=test-worker, stream=true, messages and max_tokens in [1,128]", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		controller := http.NewResponseController(w)
		produced := 0
		outcome := "cancelled"
		defer func() { logger.Info("test stream ended", "outcome", outcome, "chunks", produced) }()
		emit := func(content string, finish any) error {
			chunk := map[string]any{
				"object": "chat.completion.chunk", "model": "test-worker",
				"choices": []any{map[string]any{
					"index": 0, "delta": map[string]string{"content": content}, "finish_reason": finish,
				}},
			}
			data, err := json.Marshal(chunk)
			if err != nil {
				return err
			}
			if _, err = fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return err
			}
			return controller.Flush()
		}
		for i := 0; i < input.MaxTokens; i++ {
			if i > 0 {
				timer := time.NewTimer(interval)
				select {
				case <-r.Context().Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if r.Context().Err() != nil {
				return
			}
			if err := emit(fmt.Sprintf("test-chunk-%d ", i+1), nil); err != nil {
				outcome = "write_error"
				return
			}
			produced++
		}
		if err := emit("", "stop"); err != nil {
			outcome = "write_error"
			return
		}
		if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
			outcome = "write_error"
			return
		}
		if err := controller.Flush(); err != nil {
			outcome = "write_error"
			return
		}
		outcome = "completed"
	})
	return mux
}
