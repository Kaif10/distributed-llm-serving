// Command raftpeer runs fixed-membership Raft over loopback HTTP or mutual TLS.
// Configuration mode applies committed fleet updates; raw mode is a protocol demo.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/Kaif10/distributed-llm-serving/internal/checkpoint"
	"github.com/Kaif10/distributed-llm-serving/internal/raft"
	"github.com/Kaif10/distributed-llm-serving/internal/raftnet"
	"github.com/Kaif10/distributed-llm-serving/internal/transporttls"
)

func main() {
	id := flag.String("id", "", "local member ID")
	peers := flag.String("peers", "", "fixed IDs and origins; loopback HTTP by default, HTTPS with TLS flags")
	client := flag.String("client-listen", "127.0.0.1:19301", "separate proposal/diagnostic bind address; non-loopback requires TLS")
	journal := flag.String("journal", "", "exclusive durable journal path")
	configuration := flag.Bool("configuration", false, "serve replicated GET/PUT /config instead of raw proposals; use fresh configuration-only journals")
	peerListen := flag.String("peer-listen", "", "optional bind address; defaults to local advertised origin; non-loopback requires TLS")
	checkpointPath := flag.String("checkpoint-journal", "", "optional local checkpoint publication journal; configuration mode only")
	checkpointGroup := flag.String("checkpoint-group", "", "explicit group label required with -checkpoint-journal")
	retainedDir := flag.String("retained-dir", "", "alternative two-slot storage directory; fixed-membership configuration mode only")
	retainedGroup := flag.String("retained-group", "", "explicit group identity required with -retained-dir")
	tlsCert := flag.String("tls-cert", "", "peer certificate; enables mutual TLS on both listeners")
	tlsKey := flag.String("tls-key", "", "peer private key")
	tlsCA := flag.String("tls-ca", "", "dedicated cluster CA bundle for peers and authorized clients")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	credentials := transporttls.Files{Cert: *tlsCert, Key: *tlsKey, CA: *tlsCA}
	if err := runWithTLS(ctx, *id, *peers, *client, *journal, *configuration, *peerListen,
		*checkpointPath, *checkpointGroup, *retainedDir, *retainedGroup, credentials); err != nil {
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
	return membersWithTLS(local, spec, false)
}

func validAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	number, portErr := strconv.Atoi(port)
	return err == nil && portErr == nil && number > 0 && number <= 65535 && host != "" && !strings.ContainsAny(host, " /\\?#")
}

func membersWithTLS(local, spec string, secure bool) (map[string]string, error) {
	peers := make(map[string]string)
	origins := make(map[string]bool)
	scheme := "http"
	if secure {
		scheme = "https"
	}
	for _, part := range strings.Split(spec, ",") {
		id, origin, ok := strings.Cut(part, "=")
		u, err := url.Parse(origin)
		if !ok || id == "" || peers[id] != "" || err != nil || u.Scheme != scheme ||
			u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			!validAddress(u.Host) || (!secure && !loopback(u.Host)) {
			return nil, fmt.Errorf("invalid fixed peer specification")
		}
		// Normalize IP spelling so aliases cannot name the same destination twice.
		host, port, _ := net.SplitHostPort(u.Host)
		canonicalHost := strings.ToLower(host)
		if ip := net.ParseIP(host); ip != nil {
			canonicalHost = ip.String()
		}
		canonical := net.JoinHostPort(canonicalHost, port)
		if origins[canonical] {
			return nil, fmt.Errorf("duplicate peer address")
		}
		origins[canonical] = true
		peers[id] = scheme + "://" + canonical
	}
	if len(peers) > 9 || peers[local] == "" {
		return nil, fmt.Errorf("membership requires local ID and at most nine peers")
	}
	return peers, nil
}

func run(parent context.Context, id, spec, address, path string, configuration bool, peerBind, checkpointPath, checkpointGroup, retainedDir, retainedGroup string) error {
	return runWithTLS(parent, id, spec, address, path, configuration, peerBind,
		checkpointPath, checkpointGroup, retainedDir, retainedGroup, transporttls.Files{})
}

func runWithTLS(parent context.Context, id, spec, address, path string, configuration bool,
	peerBind, checkpointPath, checkpointGroup, retainedDir, retainedGroup string, credentials transporttls.Files) error {
	var security *tls.Config
	if credentials.Enabled() {
		var err error
		security, err = credentials.Load()
		if err != nil {
			return err
		}
		leaf, err := x509.ParseCertificate(security.Certificates[0].Certificate[0])
		if err != nil {
			return err
		}
		role, member, err := transporttls.Identity(leaf)
		if err != nil || role != "peer" || member != id {
			return fmt.Errorf("TLS certificate must identify local peer %q", id)
		}
	}
	if (checkpointPath == "") != (checkpointGroup == "") || (checkpointPath != "" && !configuration) {
		return fmt.Errorf("checkpoint journal requires configuration mode and an explicit checkpoint group")
	}
	peers, err := membersWithTLS(id, spec, security != nil)
	if err != nil {
		return err
	}
	if (retainedDir == "") != (retainedGroup == "") || (retainedDir != "" && (!configuration || checkpointPath != "")) {
		return fmt.Errorf("retained storage requires configuration mode, an explicit retained group, and no checkpoint journal")
	}
	if !validAddress(address) || (security == nil && !loopback(address)) || (path == "") == (retainedDir == "") {
		return fmt.Errorf("require valid client address (loopback without TLS) and exactly one storage mode")
	}
	origin, _ := url.Parse(peers[id])
	if peerBind == "" {
		peerBind = origin.Host
	}
	if !validAddress(peerBind) || (security == nil && !loopback(peerBind)) {
		return fmt.Errorf("peer bind requires valid address; non-loopback requires TLS")
	}
	var store interface {
		raft.LogStorage
		Close() error
	}
	if retainedDir != "" {
		store, err = raft.OpenRetainedStorage(retainedDir, raft.RetainedIdentity{Group: retainedGroup, Member: id})
	} else {
		store, err = raft.OpenStorage(path)
	}
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
		if retainedDir != "" {
			host, err = raftnet.NewConfigurationWithRetention(node, peers)
			if err != nil {
				return err
			}
		} else if checkpointPath != "" {
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
	if security != nil {
		host.UseTLS(security)
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
		return &http.Server{
			Handler: handler, ReadHeaderTimeout: 2 * time.Second,
			ReadTimeout: 6 * time.Second, WriteTimeout: 8 * time.Second,
			IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10,
			BaseContext: func(net.Listener) context.Context { return ctx },
		}
	}
	peerServer, clientServer := server(host.PeerHandler()), server(host.ClientHandler())
	if security != nil {
		peerListener = tls.NewListener(peerListener, security.Clone())
		clientListener = tls.NewListener(clientListener, security.Clone())
	}
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
