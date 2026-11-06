// Package raftnet serializes Raft ownership and moves messages over bounded HTTP.
package raftnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kaif10/inference-fleet/internal/raft"
)

type Snapshot struct {
	Node         raft.Status  `json:"node"`
	Commit       uint64       `json:"commit_index"`
	Log          []raft.Entry `json:"log"`
	Dropped      uint64       `json:"dropped_outbound"`
	SendFailures int64        `json:"send_failures"`
}
type result struct {
	State       *Snapshot
	Index, Term uint64
	Err         string
	Code        int
}
type event struct {
	ctx     context.Context
	message *raft.Message
	data    []byte
	status  bool
	reply   chan result
}
type waiter struct {
	ctx         context.Context
	index, term uint64
	reply       chan result
}
type Host struct {
	node         *raft.Node
	id           string
	peers        map[string]string
	events       chan event
	out          map[string]chan raft.Message
	done         chan struct{}
	sendFailures atomic.Int64
	dropped      uint64
}

// Node must not be accessed elsewhere after handing it to Run.
func New(node *raft.Node, peers map[string]string) *Host {
	h := &Host{node: node, id: node.Status().ID, peers: make(map[string]string), events: make(chan event, 32), out: make(map[string]chan raft.Message), done: make(chan struct{})}
	for id, origin := range peers {
		h.peers[id] = origin
		if id != h.id {
			h.out[id] = make(chan raft.Message, 2)
		}
	}
	return h
}
func (h *Host) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer close(h.done)
	var workers sync.WaitGroup
	transport := &http.Transport{MaxConnsPerHost: 1, MaxIdleConns: 9, MaxIdleConnsPerHost: 1, IdleConnTimeout: 15 * time.Second, MaxResponseHeaderBytes: 16 << 10}
	client := &http.Client{Transport: transport, Timeout: 300 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for id, queue := range h.out {
		origin := h.peers[id]
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case m := <-queue:
					data, err := json.Marshal(m)
					if err != nil {
						h.sendFailures.Add(1)
						continue
					}
					req, err := http.NewRequestWithContext(ctx, "POST", origin+"/raft/message", bytes.NewReader(data))
					if err != nil {
						h.sendFailures.Add(1)
						continue
					}
					req.Header.Set("Content-Type", "application/json")
					response, err := client.Do(req)
					if err != nil {
						h.sendFailures.Add(1)
						continue
					}
					if response.StatusCode != 204 {
						h.sendFailures.Add(1)
					}
					response.Body.Close()
				}
			}
		}()
	}
	defer func() { cancel(); workers.Wait(); client.CloseIdleConnections() }()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var pending []waiter
	emit := func(messages []raft.Message) {
		for _, m := range messages {
			if queue, ok := h.out[m.To]; ok {
				select {
				case queue <- m:
				default:
					h.dropped++
				}
			}
		}
	}
	complete := func() {
		kept := pending[:0]
		for _, w := range pending {
			if w.ctx.Err() != nil {
				continue
			}
			if h.node.CommitIndex() >= w.index {
				log := h.node.Log()
				if log[w.index-1].Term == w.term {
					w.reply <- result{Index: w.index, Term: w.term, Code: 200}
					continue
				}
			}
			if h.node.Status().Role != raft.Leader || h.node.Status().Term != w.term {
				w.reply <- result{Err: "leadership changed; outcome unknown", Code: 503}
				continue
			}
			kept = append(kept, w)
		}
		pending = kept
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			messages, err := h.node.Tick()
			if err != nil {
				return err
			}
			emit(messages)
			complete()
		case e := <-h.events:
			if e.ctx.Err() != nil {
				continue
			}
			if e.status {
				e.reply <- result{State: &Snapshot{Node: h.node.Status(), Commit: h.node.CommitIndex(), Log: h.node.Log(), Dropped: h.dropped, SendFailures: h.sendFailures.Load()}, Code: 200}
				continue
			}
			if e.message != nil {
				messages, err := h.node.Step(*e.message)
				if errors.Is(err, raft.ErrStorage) {
					return err
				}
				if err != nil {
					e.reply <- result{Err: err.Error(), Code: 400}
				} else {
					emit(messages)
					e.reply <- result{Code: 204}
				}
				complete()
				continue
			}
			complete()
			if len(pending) >= 8 {
				e.reply <- result{Err: "proposal waiters full", Code: 503}
				continue
			}
			messages, err := h.node.Propose(e.data)
			if err != nil {
				if errors.Is(err, raft.ErrStorage) {
					return err
				}
				e.reply <- result{Err: err.Error(), Code: 503}
				continue
			}
			log := h.node.Log()
			last := log[len(log)-1]
			pending = append(pending, waiter{ctx: e.ctx, index: last.Index, term: last.Term, reply: e.reply})
			emit(messages)
			complete()
		}
	}
}

func (h *Host) call(ctx context.Context, e event) result {
	e.ctx = ctx
	e.reply = make(chan result, 1)
	select {
	case h.events <- e:
	case <-ctx.Done():
		return result{Err: "deadline before processing; outcome unknown", Code: 504}
	case <-h.done:
		return result{Err: "peer stopped; outcome unknown", Code: 503}
	default:
		return result{Err: "peer event queue full", Code: 503}
	}
	select {
	case response := <-e.reply:
		return response
	case <-ctx.Done():
		return result{Err: "deadline; outcome unknown", Code: 504}
	case <-h.done:
		return result{Err: "peer stopped; outcome unknown", Code: 503}
	}
}

func bounded(handler http.Handler) http.Handler {
	slots := make(chan struct{}, 8)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			handler.ServeHTTP(w, r)
		default:
			http.Error(w, "listener admission full", 503)
		}
	})
}
func write(w http.ResponseWriter, response result) {
	if response.Err != "" {
		http.Error(w, response.Err, response.Code)
		return
	}
	if response.Code == 204 {
		w.WriteHeader(204)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if response.State != nil {
		json.NewEncoder(w).Encode(response.State)
	} else {
		json.NewEncoder(w).Encode(map[string]any{"committed": true, "index": response.Index, "term": response.Term})
	}
}
func (h *Host) PeerHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/message", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var m raft.Message
		if err := decoder.Decode(&m); err != nil {
			http.Error(w, "invalid or oversized peer message", 400)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "expected one message", 400)
			return
		}
		if _, ok := h.peers[m.From]; !ok || m.From == h.id || m.To != h.id {
			http.Error(w, "invalid claimed peer IDs", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		write(w, h.call(ctx, event{message: &m}))
	})
	return bounded(mux)
}
func (h *Host) ClientHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		write(w, h.call(ctx, event{status: true}))
	})
	mux.HandleFunc("POST /propose", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		data, err := io.ReadAll(r.Body)
		if err != nil || len(data) == 0 {
			http.Error(w, "proposal requires 1..65536 bytes", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		write(w, h.call(ctx, event{data: data}))
	})
	return bounded(mux)
}
