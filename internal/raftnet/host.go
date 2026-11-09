// Package raftnet serializes Raft ownership and moves messages over bounded HTTP.
package raftnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kaif10/distributed-llm-serving/internal/checkpoint"
	"github.com/Kaif10/distributed-llm-serving/internal/control"
	"github.com/Kaif10/distributed-llm-serving/internal/raft"
)

type Snapshot struct {
	Node          raft.Status       `json:"node"`
	Commit        uint64            `json:"commit_index"`
	Log           []raft.Entry      `json:"log"`
	Dropped       uint64            `json:"dropped_outbound"`
	SendFailures  int64             `json:"send_failures"`
	Applied       uint64            `json:"applied_index,omitempty"`
	Configuration *control.Snapshot `json:"configuration,omitempty"`
	PendingReads  int               `json:"pending_reads"`
	Checkpoint    *CheckpointStatus `json:"checkpoint,omitempty"`
	Retention     *RetentionStatus  `json:"retention,omitempty"`
}
type result struct {
	State         *Snapshot
	Index, Term   uint64
	Err           string
	Code          int
	Configuration *control.Snapshot
	Checkpoint    *CheckpointStatus
	Retention     *RetentionStatus
}
type event struct {
	ctx     context.Context
	message *raft.Message
	data    []byte
	status  bool
	read    bool
	capture bool
	retain  bool
	reply   chan result
}
type waiter struct {
	ctx         context.Context
	index, term uint64
	reply       chan result
}
type Host struct {
	node             *raft.Node
	id               string
	peers            map[string]string
	events           chan event
	out              map[string]chan raft.Message
	done             chan struct{}
	sendFailures     atomic.Int64
	dropped          uint64
	machine          *control.Machine
	applied          uint64
	checkpoints      *checkpoint.Store
	checkpointStatus *CheckpointStatus
	recovery         *checkpointRecovery
	retention        *RetentionStatus
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
	defer close(h.done)
	if h.node.BaseIndex() != 0 && (h.retention == nil || h.machine == nil || !h.node.CanRetainLocal() || h.applied != h.node.BaseIndex()) {
		return fmt.Errorf("retained snapshots require runtime installation support")
	}
	ctx, cancel := context.WithCancel(parent)
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
	var reads []readWaiter
	defer func() {
		for _, w := range reads {
			h.node.ForgetRead(w.token)
		}
	}()
	emit := func(messages []raft.Message) {
		for _, m := range messages {
			if h.retention != nil {
				m.Group = h.node.RetentionIdentity().Group
			}
			if queue, ok := h.out[m.To]; ok {
				select {
				case queue <- m:
				default:
					h.dropped++
				}
			}
		}
	}
	complete := func() error {
		applied, err := h.applyCommitted()
		if err != nil {
			return err
		}
		kept := pending[:0]
		for _, w := range pending {
			if w.ctx.Err() != nil {
				continue
			}
			if h.node.CommitIndex() >= w.index {
				if term, ok := h.node.TermAt(w.index); ok && term == w.term {
					if h.machine != nil {
						r, ok := applied[w.index]
						if !ok {
							return errors.New("missing applied command result")
						}
						w.reply <- r
					} else {
						w.reply <- result{Index: w.index, Term: w.term, Code: 200}
					}
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
		reads, err = h.finishReads(reads)
		return err
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
			if err := complete(); err != nil {
				return err
			}
		case e := <-h.events:
			if e.ctx.Err() != nil {
				continue
			}
			if e.status {
				state := &Snapshot{Node: h.node.Status(), Commit: h.node.CommitIndex(), Log: h.node.Log(), Dropped: h.dropped, SendFailures: h.sendFailures.Load(), Applied: h.applied, PendingReads: len(reads)}
				state.Checkpoint = h.checkpointSnapshot()
				state.Retention = h.retentionSnapshot()
				if h.machine != nil {
					configuration := h.machine.Snapshot()
					state.Configuration = &configuration
				}
				e.reply <- result{State: state, Code: 200}
				continue
			}
			if e.message != nil {
				if (h.retention == nil && e.message.Group != "") || (h.retention != nil && e.message.Group != h.node.RetentionIdentity().Group) {
					e.reply <- result{Err: "peer group/storage mode mismatch", Code: 400}
					continue
				}
				var installedMachine *control.Machine
				oldBase := h.node.BaseIndex()
				if e.message.Type == raft.InstallSnapshot {
					var err error
					installedMachine, err = h.validateSnapshot(*e.message)
					if err != nil {
						e.reply <- result{Err: err.Error(), Code: 400}
						continue
					}
				}
				if h.machine != nil {
					if err := validateConfigurationEntries(e.message.Entries); err != nil {
						e.reply <- result{Err: err.Error(), Code: 400}
						continue
					}
				}
				messages, err := h.node.Step(*e.message)
				if errors.Is(err, raft.ErrStorage) {
					return err
				}
				if err != nil {
					e.reply <- result{Err: err.Error(), Code: 400}
				} else {
					if h.node.BaseIndex() > oldBase {
						if installedMachine == nil {
							return fmt.Errorf("snapshot installed without application")
						}
						h.machine, h.applied = installedMachine, h.node.BaseIndex()
						h.retention = retentionMetadata(h.node.Snapshot(), h.retention.RecoveredIndex)
						h.retention.InstalledIndex = h.applied
					}
					emit(messages)
					e.reply <- result{Code: 204}
				}
				if err := complete(); err != nil {
					return err
				}
				continue
			}
			if err := complete(); err != nil {
				return err
			}
			if e.read {
				if h.machine == nil || len(reads) >= 8 {
					e.reply <- result{Err: "configuration reads unavailable or full", Code: 503}
					continue
				}
				token, messages, err := h.node.BeginRead()
				if err != nil {
					if errors.Is(err, raft.ErrStorage) {
						return err
					}
					e.reply <- result{Err: err.Error(), Code: 503}
					continue
				}
				reads = append(reads, readWaiter{ctx: e.ctx, token: token, reply: e.reply})
				emit(messages)
				if err := complete(); err != nil {
					return err
				}
				continue
			}
			if e.retain {
				response, err := h.retainLocal()
				if err != nil {
					return err
				}
				e.reply <- response
				continue
			}
			if e.capture {
				response, err := h.captureCheckpoint()
				if err != nil {
					return err
				}
				e.reply <- response
				continue
			}
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
			if err := complete(); err != nil {
				return err
			}
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
	w.Header().Set("Cache-Control", "no-store")
	if response.Err != "" {
		http.Error(w, response.Err, response.Code)
		return
	}
	if response.Code == 204 {
		w.WriteHeader(204)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if response.Configuration != nil {
		json.NewEncoder(w).Encode(response.Configuration)
	} else if response.Checkpoint != nil {
		json.NewEncoder(w).Encode(response.Checkpoint)
	} else if response.Retention != nil {
		json.NewEncoder(w).Encode(response.Retention)
	} else if response.State != nil {
		json.NewEncoder(w).Encode(response.State)
	} else {
		json.NewEncoder(w).Encode(map[string]any{"committed": true, "index": response.Index, "term": response.Term})
	}
}
func (h *Host) PeerHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/message", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 12<<20)
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
	if h.machine != nil {
		h.configurationRoutes(mux)
		return bounded(mux)
	}
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
