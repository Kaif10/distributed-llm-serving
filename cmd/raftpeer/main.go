// Command raftpeer runs a fixed-membership Raft journal over loopback HTTP.
// Configuration mode applies committed fleet updates; raw mode is a protocol demo.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Kaif10/inference-fleet/internal/checkpoint"
	"github.com/Kaif10/inference-fleet/internal/raft"
	"github.com/Kaif10/inference-fleet/internal/raftnet"
)

func main() {
	id := flag.String("id", "", "local member ID")
	peers := flag.String("peers", "", "fixed IDs and loopback origins: a=http://127.0.0.1:19201,b=http://127.0.0.1:19202")
	client := flag.String("client-listen", "127.0.0.1:19301", "separate loopback proposal/diagnostic address")
	journal := flag.String("journal", "", "exclusive durable journal path")
	configuration := flag.Bool("configuration", false, "serve replicated GET/PUT /config instead of raw proposals; use fresh configuration-only journals")
	peerListen := flag.String("peer-listen", "", "optional loopback bind address behind a test relay; defaults to local advertised origin")
	checkpointPath := flag.String("checkpoint-journal", "", "optional local checkpoint publication journal; configuration mode only")
	checkpointGroup := flag.String("checkpoint-group", "", "explicit group label required with -checkpoint-journal")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *id, *peers, *client, *journal, *configuration, *peerListen, *checkpointPath, *checkpointGroup); err != nil {
		slog.Error("raft peer stopped", "error", err)
		os.Exit(1)
	}
}

func loopback(address string) bool {
	host, port, err := net.SplitHostPort(address)
	portNumber, portErr := strconv.Atoi(port)
	return err == nil && portErr == nil && portNumber > 0 && portNumber <= 65535 && net.ParseIP(host).IsLoopback()
}
func members(local, spec string) (map[string]string, error) {
	peers := make(map[string]string)
	origins := make(map[string]bool)
	for _, part := range strings.Split(spec, ",") {
		id, origin, ok := strings.Cut(part, "=")
		u, err := url.Parse(origin)
		if !ok || id == "" || peers[id] != "" || err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || !loopback(u.Host) {
			return nil, fmt.Errorf("invalid fixed peer specification")
		}
		// Normalize IP spelling so aliases cannot name the same destination twice.
		host, port, _ := net.SplitHostPort(u.Host)
		canonical := net.JoinHostPort(net.ParseIP(host).String(), port)
		if origins[canonical] {
			return nil, fmt.Errorf("duplicate peer address")
		}
		origins[canonical] = true
		peers[id] = "http://" + canonical
	}
	if len(peers) > 9 || peers[local] == "" {
		return nil, fmt.Errorf("membership requires local ID and at most nine peers")
	}
	return peers, nil
}

func run(parent context.Context, id, spec, address, path string, configuration bool, peerBind, checkpointPath, checkpointGroup string) error {
	if (checkpointPath == "") != (checkpointGroup == "") || (checkpointPath != "" && !configuration) {
		return fmt.Errorf("checkpoint journal requires configuration mode and an explicit checkpoint group")
	}
	peers, err := members(id, spec)
	if err != nil {
		return err
	}
	if !loopback(address) || path == "" {
		return fmt.Errorf("require loopback client address and journal path")
	}
	store, err := raft.OpenStorage(path)
	if err != nil {
		return err
	}
	defer store.Close()
	var entropy [8]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return err
	}
	seed := int64(binary.LittleEndian.Uint64(entropy[:]))
	if seed == 0 {
		seed = 1
	}
	ids := make([]string, 0, len(peers))
	for peer := range peers {
		ids = append(ids, peer)
	}
	node, err := raft.NewReplica(raft.Config{ID: id, Members: ids, ElectionTicks: 10, HeartbeatTicks: 2, Seed: seed}, store)
	if err != nil {
		return err
	}
	host := raftnet.New(node, peers)
	if configuration {
		if checkpointPath != "" {
			publications, err := checkpoint.Open(checkpointPath, checkpoint.Identity{Group: checkpointGroup, Member: id})
			if err != nil {
				return err
			}
			defer publications.Close()
			host, err = raftnet.NewConfigurationWithCheckpoints(node, peers, publications)
			if err != nil {
				return err
			}
		} else {
			host, err = raftnet.NewConfiguration(node, peers)
			if err != nil {
				return err
			}
		}
	}
	origin, _ := url.Parse(peers[id])
	if peerBind == "" {
		peerBind = origin.Host
	}
	if !loopback(peerBind) {
		return fmt.Errorf("peer bind requires loopback address")
	}
	peerListener, err := net.Listen("tcp", peerBind)
	if err != nil {
		return err
	}
	defer peerListener.Close()
	clientListener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer clientListener.Close()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	server := func(handler http.Handler) *http.Server {
		return &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 6 * time.Second, WriteTimeout: 8 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	}
	peerServer, clientServer := server(host.PeerHandler()), server(host.ClientHandler())
	defer peerServer.Close()
	defer clientServer.Close()
	runtimeDone := make(chan error, 1)
	serveDone := make(chan error, 2)
	go func() { runtimeDone <- host.Run(ctx) }()
	go func() { serveDone <- peerServer.Serve(peerListener) }()
	go func() { serveDone <- clientServer.Serve(clientListener) }()
	slog.Info("raft peer listening", "id", id, "peer", peerListener.Addr(), "client", clientListener.Addr())
	var outcome error
	runtimeStopped := false
	select {
	case <-parent.Done():
	case outcome = <-runtimeDone:
		runtimeStopped = true
	case outcome = <-serveDone:
	}
	cancel()
	// Cancel active handlers before joining the owner and releasing the journal.
	peerServer.Close()
	clientServer.Close()
	if !runtimeStopped {
		if err := <-runtimeDone; outcome == nil {
			outcome = err
		}
	}
	return outcome
}
