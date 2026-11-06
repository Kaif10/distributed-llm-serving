package gateway

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestAffinityStableAndEscapesIneligibleWorkers(t *testing.T) {
	h, err := NewPoolHandler(PoolOptions{Backends: []string{"http://127.0.0.1:1", "http://127.0.0.1:2"}, Policy: "prefix-affinity", MaxInflight: 1}, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	p := h.pool
	preferred, ok := p.acquire("document-A")
	if !ok {
		t.Fatal("no preferred worker")
	}
	other, ok := p.acquire("document-A")
	if !ok || other == preferred {
		t.Fatal("did not escape full preferred worker")
	}
	if _, ok := p.acquire("document-A"); ok {
		t.Fatal("exceeded capacity")
	}
	p.release(preferred)
	p.release(other)
	for i := 0; i < 10; i++ {
		got, _ := p.acquire("document-A")
		if got != preferred {
			t.Fatal("unstable affinity")
		}
		p.release(got)
	}
	for _, drain := range []bool{false, true} {
		p.states[preferred].Healthy = drain
		p.states[preferred].Draining = drain
		got, ok := p.acquire("document-A")
		if !ok || got != other {
			t.Fatal("ineligible preferred worker selected")
		}
		p.release(got)
	}
}
