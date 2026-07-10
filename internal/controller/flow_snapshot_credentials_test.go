package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolvedFlowArtifactCredentialsLoadsSSHAndClientCert(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "src", Namespace: "default"},
		Data: map[string][]byte{
			"ssh-key":     []byte("-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----\n"),
			"known_hosts": []byte("github.com ssh-ed25519 AAAA\n"),
			"tls.crt":     []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"),
			"tls.key":     []byte("-----BEGIN EC PRIVATE KEY-----\nMHc\n-----END EC PRIVATE KEY-----\n"),
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(secret).Build()

	t.Run("ssh git credentials", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{
			URL: "git@github.com:org/repo.git",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{
				SSHPrivateKeySecretKeyRef: srcRef("src", "ssh-key"),
				SSHKnownHostsSecretKeyRef: srcRef("src", "known_hosts"),
			},
		}}
		creds, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source)
		if err != nil {
			t.Fatal(err)
		}
		if len(creds.SSHPrivateKey) == 0 || len(creds.SSHKnownHosts) == 0 {
			t.Fatalf("SSH material not resolved: key=%d knownHosts=%d", len(creds.SSHPrivateKey), len(creds.SSHKnownHosts))
		}
	})

	t.Run("ssh on a non-git source is rejected", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "b", FlowID: "f",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{SSHPrivateKeySecretKeyRef: srcRef("src", "ssh-key")},
		}}
		if _, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source); err == nil {
			t.Fatal("expected SSH auth on a registry source to be rejected")
		}
	})

	t.Run("client certificate on a git source is rejected", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{
			URL: "https://git.example.com/flows.git",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{
				ClientCertificateSecretKeyRef: srcRef("src", "tls.crt"),
				ClientKeySecretKeyRef:         srcRef("src", "tls.key"),
			},
		}}
		if _, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source); err == nil {
			t.Fatal("expected client certificate auth on a git source to be rejected")
		}
	})

	t.Run("registry client certificate", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "b", FlowID: "f",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{
				ClientCertificateSecretKeyRef: srcRef("src", "tls.crt"),
				ClientKeySecretKeyRef:         srcRef("src", "tls.key"),
			},
		}}
		creds, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source)
		if err != nil {
			t.Fatal(err)
		}
		if len(creds.ClientCertData) == 0 || len(creds.ClientKeyData) == 0 {
			t.Fatal("client certificate material not resolved")
		}
	})
}

func TestResolvedFlowArtifactCredentialsOIDCExchange(t *testing.T) {
	tokenServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "expected POST", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"oidc-bearer","token_type":"Bearer","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oidc", Namespace: "default"},
		Data:       map[string][]byte{"id": []byte("client-id"), "secret": []byte("client-secret")},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(secret).Build()

	t.Run("registry source exchanges for a bearer token", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "b", FlowID: "f",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{
				// Trust the test server's self-signed cert for the token exchange.
				InsecureSkipVerify: true,
				OIDC: &nifiv1alpha1.FlowArtifactOIDC{
					TokenURL:                 tokenServer.URL,
					ClientIDSecretKeyRef:     srcRef("oidc", "id"),
					ClientSecretSecretKeyRef: srcRef("oidc", "secret"),
					Scopes:                   []string{"registry.read"},
				},
			},
		}}
		creds, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source)
		if err != nil {
			t.Fatal(err)
		}
		if creds.Token != "oidc-bearer" {
			t.Fatalf("expected the OIDC bearer token, got %q", creds.Token)
		}
	})

	t.Run("oidc is rejected for non-registry sources", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{
			URL: "https://git.example.com/flows.git",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{
				OIDC: &nifiv1alpha1.FlowArtifactOIDC{
					TokenURL:                 tokenServer.URL,
					ClientIDSecretKeyRef:     srcRef("oidc", "id"),
					ClientSecretSecretKeyRef: srcRef("oidc", "secret"),
				},
			},
		}}
		if _, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source); err == nil {
			t.Fatal("expected OIDC on a git source to be rejected")
		}
	})
}
