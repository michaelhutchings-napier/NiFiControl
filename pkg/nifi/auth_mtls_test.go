package nifi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testKeyPair struct {
	certPEM []byte
	keyPEM  []byte
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) testKeyPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "NiFiControl Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testKeyPair{certPEM: pemEncode("CERTIFICATE", der), keyPEM: ecKeyPEM(t, key), cert: cert, key: key}
}

func newTestLeaf(t *testing.T, ca testKeyPair, commonName string, eku []x509.ExtKeyUsage, dnsNames []string, ips []net.IP) testKeyPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  eku,
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testKeyPair{certPEM: pemEncode("CERTIFICATE", der), keyPEM: ecKeyPEM(t, key), cert: cert, key: key}
}

func pemEncode(blockType string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
}

func ecKeyPEM(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pemEncode("EC PRIVATE KEY", der)
}

// newMutualTLSServer starts a server that requires and verifies client certificates
// signed by ca, recording the presented client common name.
func newMutualTLSServer(t *testing.T, ca, server testKeyPair, seenCN *string) *httptest.Server {
	t.Helper()
	serverCert, err := tls.X509KeyPair(server.certPEM, server.keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(ca.certPEM) {
		t.Fatal("failed to add CA to pool")
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) > 0 && seenCN != nil {
			*seenCN = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"about":{"version":"2.10.0"}}`))
	})
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestNewHTTPClientPresentsClientCertificateOverMutualTLS(t *testing.T) {
	ca := newTestCA(t)
	server := newTestLeaf(t, ca, "production-node", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	client := newTestLeaf(t, ca, "production-operator", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)

	var seenCN string
	srv := newMutualTLSServer(t, ca, server, &seenCN)

	httpClient, err := NewHTTPClient(HTTPClientConfig{
		BaseURI:        srv.URL,
		CAData:         ca.certPEM,
		ClientCertData: client.certPEM,
		ClientKeyData:  client.keyPEM,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (HTTPReachabilityChecker{Client: httpClient}).CheckReachable(t.Context(), srv.URL, time.Second); err != nil {
		t.Fatalf("mTLS request failed: %v", err)
	}
	if seenCN != "production-operator" {
		t.Fatalf("server saw client CN %q, want production-operator", seenCN)
	}
}

func TestNewHTTPClientRejectedWithoutClientCertificate(t *testing.T) {
	ca := newTestCA(t)
	server := newTestLeaf(t, ca, "production-node", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	srv := newMutualTLSServer(t, ca, server, nil)

	// Trusts the server CA but presents no client certificate: the server must reject it.
	httpClient, err := NewHTTPClient(HTTPClientConfig{BaseURI: srv.URL, CAData: ca.certPEM})
	if err != nil {
		t.Fatal(err)
	}
	if err := (HTTPReachabilityChecker{Client: httpClient}).CheckReachable(t.Context(), srv.URL, time.Second); err == nil {
		t.Fatal("expected the server to reject a request without a client certificate")
	}
}

func TestNewHTTPClientValidatesCustomServerCA(t *testing.T) {
	ca := newTestCA(t)
	server := newTestLeaf(t, ca, "production-node", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	client := newTestLeaf(t, ca, "production-operator", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)
	srv := newMutualTLSServer(t, ca, server, nil)

	// Without the custom CA the server certificate is signed by an unknown authority.
	withoutCA, err := NewHTTPClient(HTTPClientConfig{BaseURI: srv.URL, ClientCertData: client.certPEM, ClientKeyData: client.keyPEM})
	if err != nil {
		t.Fatal(err)
	}
	if err := (HTTPReachabilityChecker{Client: withoutCA}).CheckReachable(t.Context(), srv.URL, time.Second); err == nil {
		t.Fatal("expected verification failure without the custom CA")
	}

	withCA, err := NewHTTPClient(HTTPClientConfig{BaseURI: srv.URL, CAData: ca.certPEM, ClientCertData: client.certPEM, ClientKeyData: client.keyPEM})
	if err != nil {
		t.Fatal(err)
	}
	if err := (HTTPReachabilityChecker{Client: withCA}).CheckReachable(t.Context(), srv.URL, time.Second); err != nil {
		t.Fatalf("custom CA validation failed: %v", err)
	}
}

func TestRegisterHTTPClientRebuiltOnRotation(t *testing.T) {
	ca := newTestCA(t)
	server := newTestLeaf(t, ca, "production-node", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	oldClient := newTestLeaf(t, ca, "operator-old", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)
	newClient := newTestLeaf(t, ca, "operator-new", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)

	var seenCN string
	srv := newMutualTLSServer(t, ca, server, &seenCN)

	build := func(client testKeyPair) *http.Client {
		httpClient, err := NewHTTPClient(HTTPClientConfig{BaseURI: srv.URL, CAData: ca.certPEM, ClientCertData: client.certPEM, ClientKeyData: client.keyPEM})
		if err != nil {
			t.Fatal(err)
		}
		return httpClient
	}

	if err := RegisterHTTPClient(srv.URL, build(oldClient)); err != nil {
		t.Fatal(err)
	}
	if err := (HTTPReachabilityChecker{}).CheckReachable(t.Context(), srv.URL, time.Second); err != nil {
		t.Fatal(err)
	}
	if seenCN != "operator-old" {
		t.Fatalf("server saw %q before rotation, want operator-old", seenCN)
	}

	// Rotation: re-register a client built from the new certificate material.
	if err := RegisterHTTPClient(srv.URL, build(newClient)); err != nil {
		t.Fatal(err)
	}
	if err := (HTTPReachabilityChecker{}).CheckReachable(t.Context(), srv.URL, time.Second); err != nil {
		t.Fatal(err)
	}
	if seenCN != "operator-new" {
		t.Fatalf("server saw %q after rotation, want operator-new", seenCN)
	}
}

func TestNewHTTPClientRejectsCombinedAuthModes(t *testing.T) {
	ca := newTestCA(t)
	client := newTestLeaf(t, ca, "operator", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil)

	if _, err := NewHTTPClient(HTTPClientConfig{BaseURI: "https://nifi.example.com", ClientCertData: client.certPEM, ClientKeyData: client.keyPEM, BearerToken: "token"}); err == nil {
		t.Fatal("expected mTLS + bearer token to be rejected")
	}
	if _, err := NewHTTPClient(HTTPClientConfig{BaseURI: "https://nifi.example.com", BearerToken: "token", Username: "u", Password: "p"}); err == nil {
		t.Fatal("expected bearer + username/password to be rejected")
	}
}
