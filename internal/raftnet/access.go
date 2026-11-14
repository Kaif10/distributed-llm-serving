package raftnet

import (
	"fmt"
	"net/http"

	"github.com/Kaif10/distributed-llm-serving/internal/transporttls"
)

func authenticated(r *http.Request) (string, string, error) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		return "", "", fmt.Errorf("mutual TLS required")
	}
	return transporttls.Identity(r.TLS.PeerCertificates[0])
}

func (h *Host) clientAccess(next http.Handler) http.Handler {
	if h.tlsConfig == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role, _, err := authenticated(r)
		read := r.Method == "GET" && (r.URL.Path == "/config" || r.URL.Path == "/status")
		if err != nil || (role != "admin" && !(role == "reader" && read)) {
			http.Error(w, "admin certificate required for writes; reader required for reads", 403)
			return
		}
		next.ServeHTTP(w, r)
	})
}
