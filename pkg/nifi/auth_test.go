package nifi

import (
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientExchangesAndCachesNiFiTokenWithCustomCA(t *testing.T) {
	tokenRequests := 0
	token := testJWT(time.Now().Add(time.Hour))
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/nifi-api/access/token":
			tokenRequests++
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("username") != "operator" || r.Form.Get("password") != "secret" {
				t.Fatalf("credentials = %q/%q", r.Form.Get("username"), r.Form.Get("password"))
			}
			_, _ = w.Write([]byte(token))
		case "/nifi-api/flow/about":
			if r.Header.Get("Authorization") != "Bearer "+token {
				t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
			}
			_, _ = w.Write([]byte(`{"about":{"version":"2.10.0"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	certificate := server.Certificate()
	caData := pemCertificate(certificate)
	client, err := NewHTTPClient(HTTPClientConfig{
		BaseURI: server.URL, CAData: caData, Username: "operator", Password: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	checker := HTTPReachabilityChecker{Client: client}
	for range 2 {
		if err := checker.CheckReachable(t.Context(), server.URL, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if tokenRequests != 1 {
		t.Fatalf("token requests = %d, want 1", tokenRequests)
	}
}

func TestHTTPClientUsesStaticBearerToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixed-token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewHTTPClient(HTTPClientConfig{BaseURI: server.URL, BearerToken: "fixed-token"})
	if err != nil {
		t.Fatal(err)
	}
	if err := (HTTPReachabilityChecker{Client: client}).CheckReachable(t.Context(), server.URL, time.Second); err != nil {
		t.Fatal(err)
	}
}

func testJWT(expiry time.Time) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, expiry.Unix())))
	return header + "." + payload + ".signature"
}

func pemCertificate(certificate *x509.Certificate) []byte {
	return []byte("-----BEGIN CERTIFICATE-----\n" +
		base64.StdEncoding.EncodeToString(certificate.Raw) +
		"\n-----END CERTIFICATE-----\n")
}
