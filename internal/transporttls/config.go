// Package transporttls loads cluster-scoped mutual TLS credentials. Certificate
// identities are exact URI SANs; certificates still require ordinary CA, expiry,
// EKU and server hostname verification. A dedicated CA is the cluster boundary.
package transporttls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
)

type Files struct{ Cert, Key, CA string }

func (f Files) Enabled() bool { return f.Cert != "" || f.Key != "" || f.CA != "" }

func Identity(cert *x509.Certificate) (role, id string, err error) {
	if cert == nil || len(cert.URIs) != 1 {
		return "", "", fmt.Errorf("require one fleet URI SAN")
	}
	u := cert.URIs[0]
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if u.Scheme != "spiffe" || u.Host != "distributed-llm-serving" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(parts) != 2 || parts[1] == "" {
		return "", "", fmt.Errorf("invalid fleet URI SAN")
	}
	if parts[0] != "peer" && parts[0] != "admin" && parts[0] != "reader" {
		return "", "", fmt.Errorf("invalid fleet role")
	}
	return parts[0], parts[1], nil
}

func (f Files) Load() (*tls.Config, error) {
	if f.Cert == "" || f.Key == "" || f.CA == "" {
		return nil, fmt.Errorf("require all TLS certificate, key, and CA files")
	}
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	if _, _, err := Identity(leaf); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(f.CA)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("empty TLS CA bundle")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: pool,
		ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}

// PeerClient pins the expected member in addition to standard SAN verification.
func PeerClient(config *tls.Config, expected string) *tls.Config {
	c := config.Clone()
	c.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return fmt.Errorf("unverified peer")
		}
		role, id, err := Identity(state.PeerCertificates[0])
		if err != nil || role != "peer" || id != expected {
			return fmt.Errorf("TLS destination is not member %q", expected)
		}
		return nil
	}
	return c
}

// ControllerClient accepts only authenticated peer servers. Each URL still has
// normal hostname verification, while a dedicated CA establishes cluster scope.
func ControllerClient(config *tls.Config) *tls.Config {
	c := config.Clone()
	c.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return fmt.Errorf("unverified controller")
		}
		role, _, err := Identity(state.PeerCertificates[0])
		if err != nil || role != "peer" {
			return fmt.Errorf("TLS server is not a controller peer")
		}
		return nil
	}
	return c
}

func SecureOrigin(origin string) bool {
	u, err := url.Parse(origin)
	return err == nil && u.Scheme == "https" && u.Hostname() != ""
}
