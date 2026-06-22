package nifi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type HTTPClientConfig struct {
	BaseURI            string
	Timeout            time.Duration
	CAData             []byte
	ServerName         string
	InsecureSkipVerify bool
	BearerToken        string
	Username           string
	Password           string
	// ClientCertData and ClientKeyData are PEM-encoded client certificate material the
	// operator presents for mutual TLS. They are mutually exclusive with the bearer and
	// username/password modes.
	ClientCertData []byte
	ClientKeyData  []byte
}

var registeredHTTPClients sync.Map

func RegisterHTTPClient(baseURI string, client *http.Client) error {
	key, err := httpClientKey(baseURI)
	if err != nil {
		return err
	}
	registeredHTTPClients.Store(key, client)
	return nil
}

func NewHTTPClient(config HTTPClientConfig) (*http.Client, error) {
	usingClientCert := len(config.ClientCertData) > 0 || len(config.ClientKeyData) > 0
	usingBearer := config.BearerToken != ""
	usingBasic := config.Username != "" || config.Password != ""
	if usingClientCert && (usingBearer || usingBasic) {
		return nil, fmt.Errorf("mTLS client certificate authentication is mutually exclusive with bearer token and username/password")
	}
	if usingBearer && usingBasic {
		return nil, fmt.Errorf("bearer token and username/password authentication are mutually exclusive")
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil || rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	if len(config.CAData) > 0 && !rootCAs.AppendCertsFromPEM(config.CAData) {
		return nil, fmt.Errorf("configured CA data does not contain a PEM certificate")
	}
	tlsConfig := &tls.Config{
		RootCAs:            rootCAs,
		ServerName:         config.ServerName,
		InsecureSkipVerify: config.InsecureSkipVerify, // #nosec G402 -- explicitly configured by the cluster owner.
	}
	if usingClientCert {
		certificate, err := tls.X509KeyPair(config.ClientCertData, config.ClientKeyData)
		if err != nil {
			return nil, fmt.Errorf("load client certificate key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	var roundTripper http.RoundTripper = transport
	if usingBearer || usingBasic {
		roundTripper = &bearerTokenRoundTripper{
			baseURI:     config.BaseURI,
			transport:   transport,
			staticToken: config.BearerToken,
			username:    config.Username,
			password:    config.Password,
		}
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}
	return &http.Client{Transport: roundTripper, Timeout: timeout}, nil
}

type bearerTokenRoundTripper struct {
	baseURI     string
	transport   http.RoundTripper
	staticToken string
	username    string
	password    string

	mutex  sync.Mutex
	token  string
	expiry time.Time
}

func (t *bearerTokenRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	token, err := t.bearerToken(request.Context())
	if err != nil {
		return nil, err
	}
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+token)
	return t.transport.RoundTrip(clone)
}

func (t *bearerTokenRoundTripper) bearerToken(ctx context.Context) (string, error) {
	if t.staticToken != "" {
		return t.staticToken, nil
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.token != "" && (t.expiry.IsZero() || time.Now().Add(time.Minute).Before(t.expiry)) {
		return t.token, nil
	}
	endpoint, err := apiURL(t.baseURI, "/access/token")
	if err != nil {
		return "", err
	}
	form := url.Values{"username": {t.username}, "password": {t.password}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := t.transport.RoundTrip(req)
	if err != nil {
		return "", fmt.Errorf("request NiFi access token: %w", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return "", fmt.Errorf("NiFi access token endpoint returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	t.token = strings.TrimSpace(string(payload))
	if t.token == "" {
		return "", fmt.Errorf("NiFi access token endpoint returned an empty token")
	}
	t.expiry = jwtExpiry(t.token)
	return t.token, nil
}

func jwtExpiry(token string) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}
	}
	var claims struct {
		ExpiresAt json.Number `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}
	}
	seconds, err := claims.ExpiresAt.Int64()
	if err != nil {
		return time.Time{}
	}
	return time.Unix(seconds, 0)
}

func clientForEndpoint(explicit *http.Client, endpoint string) *http.Client {
	if explicit != nil {
		return explicit
	}
	key, err := httpClientKey(endpoint)
	if err == nil {
		if registered, ok := registeredHTTPClients.Load(key); ok {
			return registered.(*http.Client)
		}
	}
	return &http.Client{Timeout: defaultTimeout}
}

func httpClientKey(rawURI string) (string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("URI must include scheme and host")
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host), nil
}
