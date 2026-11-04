package gateway

import (
	"fmt"
	"net/http/httputil"
	"sync"
)

// PoolOptions describes replicas of the same model. Limits apply to this gateway.
type PoolOptions struct {
	Backends    []string
	Policy      string
	MaxInflight int
}

type workerPool struct {
	mu       sync.Mutex
	proxies  []*httputil.ReverseProxy
	inflight []int
	next     int
	limit    int
	policy   string
}

func validateOptions(options PoolOptions) error {
	if options.Policy != "round-robin" && options.Policy != "least-inflight" {
		return fmt.Errorf("policy must be round-robin or least-inflight")
	}
	if options.MaxInflight < 1 {
		return fmt.Errorf("max-inflight must be positive")
	}
	return nil
}

// Selection and reservation share one lock, so concurrent requests cannot all
// observe an available slot and oversubscribe it. No network work holds the lock.
func (p *workerPool) acquire() (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	selected := -1
	for offset := 0; offset < len(p.proxies); offset++ {
		i := (p.next + offset) % len(p.proxies)
		if p.inflight[i] >= p.limit {
			continue
		}
		if selected == -1 || p.inflight[i] < p.inflight[selected] {
			selected = i
		}
		if p.policy == "round-robin" {
			break
		}
	}
	if selected < 0 {
		return 0, false
	}
	p.inflight[selected]++
	p.next = (selected + 1) % len(p.proxies)
	return selected, true
}

func (p *workerPool) release(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight[index]--
}
