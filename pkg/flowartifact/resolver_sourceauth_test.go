package flowartifact

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	cryptossh "golang.org/x/crypto/ssh"
)

// --- SSH authentication (unit) ------------------------------------------------

func rsaPrivateKeyPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func TestGitAuthenticationRoutesSSHvsHTTPS(t *testing.T) {
	keyPEM := rsaPrivateKeyPEM(t)
	// scp-style URL -> SSH public-key auth (host key verification skipped via the insecure flag).
	method, err := gitAuthentication("git@github.com:org/repo.git", Credentials{SSHPrivateKey: keyPEM, SSHInsecureIgnoreHostKey: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := method.(*gitssh.PublicKeys); !ok {
		t.Fatalf("expected SSH public-key auth for scp-style URL, got %T", method)
	}
	// ssh:// URL -> SSH.
	method, err = gitAuthentication("ssh://git@example.com/org/repo.git", Credentials{SSHPrivateKey: keyPEM, SSHInsecureIgnoreHostKey: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := method.(*gitssh.PublicKeys); !ok {
		t.Fatalf("expected SSH auth for ssh:// URL, got %T", method)
	}
	// https URL with an SSH key present -> still HTTP basic (the key is ignored for HTTPS).
	method, err = gitAuthentication("https://example.com/org/repo.git", Credentials{Token: "t", SSHPrivateKey: keyPEM})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := method.(*gitssh.PublicKeys); ok {
		t.Fatalf("HTTPS URL must not use SSH auth")
	}
}

func TestSSHAuthenticationRequiresKey(t *testing.T) {
	if _, err := gitAuthentication("git@host:org/repo.git", Credentials{SSHInsecureIgnoreHostKey: true}); err == nil {
		t.Fatal("expected an error when an SSH URL has no private key")
	}
}

func TestSSHHostKeyPolicy(t *testing.T) {
	keyPEM := rsaPrivateKeyPEM(t)

	// No known hosts and not explicitly insecure -> rejected (never silently trust any host key).
	if _, err := gitAuthentication("git@host:org/repo.git", Credentials{SSHPrivateKey: keyPEM}); err == nil {
		t.Fatal("expected host-key verification to be required")
	}

	// Explicit opt-out -> allowed.
	if _, err := gitAuthentication("git@host:org/repo.git", Credentials{SSHPrivateKey: keyPEM, SSHInsecureIgnoreHostKey: true}); err != nil {
		t.Fatalf("insecure opt-out should be accepted: %v", err)
	}

	// Valid known_hosts -> a real known-hosts callback is built (user defaults to git).
	_, hostPub := generateSSHHostKey(t)
	knownHosts := []byte(knownHostsLine("[127.0.0.1]:2222", hostPub))
	method, err := gitAuthentication("git@127.0.0.1:org/repo.git", Credentials{SSHPrivateKey: keyPEM, SSHKnownHosts: knownHosts})
	if err != nil {
		t.Fatal(err)
	}
	publicKeys, ok := method.(*gitssh.PublicKeys)
	if !ok {
		t.Fatalf("expected SSH public-key auth, got %T", method)
	}
	if publicKeys.User != "git" {
		t.Fatalf("expected default SSH user 'git', got %q", publicKeys.User)
	}
	if publicKeys.HostKeyCallback == nil {
		t.Fatal("expected a host-key callback from known_hosts")
	}
}

// --- Client-certificate (mutual TLS) registry auth (integration) --------------

func TestResolveRegistryWithClientCertificate(t *testing.T) {
	snapshot := map[string]any{"flowContents": map[string]any{"name": "pg"}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "no client certificate", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(snapshot)
	})
	server := httptest.NewUnstartedServer(handler)

	clientCAPool := x509.NewCertPool()
	caPEM, certPEM, keyPEM := issueClientCertificate(t)
	if !clientCAPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("failed to load client CA")
	}
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAPool}
	server.StartTLS()
	defer server.Close()

	serverCAPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	source := nifiv1alpha1.RegistryFlowSource{BucketID: "b", FlowID: "f", Version: "1"}

	// With a client certificate: the mutual-TLS handshake succeeds and the snapshot is fetched.
	resolver := DefaultResolver{}
	artifact, err := resolver.resolveRegistry(context.Background(), server.URL, source, Credentials{
		CAData: serverCAPEM, ClientCertData: certPEM, ClientKeyData: keyPEM,
	})
	if err != nil {
		t.Fatalf("expected client-certificate auth to succeed: %v", err)
	}
	if artifact == nil || len(artifact.Snapshot.Raw) == 0 {
		t.Fatal("expected a snapshot")
	}

	// Without a client certificate: the server rejects the handshake.
	if _, err := resolver.resolveRegistry(context.Background(), server.URL, source, Credentials{CAData: serverCAPEM}); err == nil {
		t.Fatal("expected the fetch to fail without a client certificate")
	}
}

// issueClientCertificate returns a CA PEM plus a client leaf certificate and key PEM signed by it.
func issueClientCertificate(t *testing.T) (caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-client-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "nificontrol-flow-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKeyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: leafKeyDER})
	return caPEM, certPEM, keyPEM
}

// generateSSHHostKey returns an SSH signer and its public key for test servers/known_hosts.
func generateSSHHostKey(t *testing.T) (cryptossh.Signer, cryptossh.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := cryptossh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return signer, signer.PublicKey()
}

func knownHostsLine(address string, hostKey cryptossh.PublicKey) string {
	return address + " " + string(cryptossh.MarshalAuthorizedKey(hostKey))
}
