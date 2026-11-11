package gateway

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http/httputil"
	"sync"
	"time"
)

// PoolOptions describes replicas of the same model. Limits apply to this gateway.
type PoolOptions struct {
	Backends      []string
	Policy        string
	MaxInflight   int
	MaxUploads    int
	UploadTimeout time.Duration
}

type workerPool struct {
	mu          sync.Mutex
	proxies     []*httputil.ReverseProxy
	inflight    []int
	next        int
	limit       int
	policy      string
	states      []WorkerState
	config      ConfigStatus
	configBytes string
}

// WorkerState is a local observation, not a fleet-wide guarantee.
type WorkerState struct {
	ID        int       `json:"id"`
	Origin    string    `json:"origin"`
	Healthy   bool      `json:"healthy"`
	CheckedAt time.Time `json:"checked_at"`
	Draining  bool      `json:"draining"`
	Inflight  int       `json:"inflight"`
	Drained   bool      `json:"drained"`
	ConfigID  string    `json:"config_id,omitempty"`
}

func validateOptions(options PoolOptions) error {
	if options.Policy != "round-robin" && options.Policy != "least-inflight" && options.Policy != "prefix-affinity" {
		return fmt.Errorf("policy must be round-robin, least-inflight, or prefix-affinity")
	}
	if options.MaxInflight < 1 {
		return fmt.Errorf("max-inflight must be positive")
	}
	if options.MaxUploads < 0 || options.MaxUploads > 1024 || options.UploadTimeout < 0 {
		return fmt.Errorf("max-uploads must be 0..1024 and upload-timeout nonnegative")
	}
	return nil
}

// Selection and reservation share one lock, so concurrent requests cannot all
// observe an available slot and oversubscribe it. No network work holds the lock.
func (p *workerPool) acquire(keys ...string) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.freshLocked() {
		return 0, false
	}
	selected := -1
	var best [32]byte
	affinity := p.policy == "prefix-affinity" && len(keys) > 0 && keys[0] != ""
	for offset := 0; offset < len(p.proxies); offset++ {
		i := (p.next + offset) % len(p.proxies)
		if len(p.states) > 0 && (!p.states[i].Healthy || p.states[i].Draining) {
			continue
		}
		if p.inflight[i] >= p.limit {
			continue
		}
		if affinity {
			score := sha256.Sum256([]byte(keys[0] + "\x00" + p.states[i].Origin))
			if selected == -1 || bytes.Compare(score[:], best[:]) > 0 {
				selected = i
				best = score
			}
		} else if selected == -1 || p.inflight[i] < p.inflight[selected] {
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
