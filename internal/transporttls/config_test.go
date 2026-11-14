package transporttls

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"
)

func TestIdentityAndDestinationPin(t *testing.T) {
	u, _ := url.Parse("spiffe://distributed-llm-serving/peer/b")
	cert := &x509.Certificate{URIs: []*url.URL{u}}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}
	config := PeerClient(&tls.Config{}, "b")
	if err := config.VerifyConnection(state); err != nil {
		t.Fatal(err)
	}
	if err := PeerClient(&tls.Config{}, "a").VerifyConnection(state); err == nil {
		t.Fatal("accepted wrong member with a shared hostname")
	}
	state.VerifiedChains = nil
	if err := config.VerifyConnection(state); err == nil {
		t.Fatal("accepted unverified destination")
	}
	for _, identity := range []string{"spiffe://wrong/peer/b", "spiffe://distributed-llm-serving/reader/b", "spiffe://distributed-llm-serving/peer/b/extra"} {
		u, _ := url.Parse(identity)
		cert.URIs = []*url.URL{u}
		state.VerifiedChains = [][]*x509.Certificate{{cert}}
		if err := config.VerifyConnection(state); err == nil {
			t.Fatal("accepted", identity)
		}
	}
	cert.URIs = []*url.URL{}
	if _, _, err := Identity(cert); err == nil {
		t.Fatal("accepted missing identity")
	}
}

func TestPartialCredentialsFailClosed(t *testing.T) {
	for _, files := range []Files{{Cert: "cert"}, {CA: "ca"}, {Cert: "cert", Key: "key"}} {
		if _, err := files.Load(); err == nil {
			t.Fatal("partial TLS credentials accepted")
		}
	}
}
