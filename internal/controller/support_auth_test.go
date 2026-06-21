package controller

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConfigureClusterHTTPClientResolvesTLSAndBearerSecrets(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nifi-api/flow/about" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer operator-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	caData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "nifi-api", Namespace: "default"},
		Data: map[string][]byte{
			"ca.crt": caData,
			"token":  []byte("operator-token\n"),
		},
	}
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode: nifiv1alpha1.ClusterModeExternal,
			API: &nifiv1alpha1.NiFiClusterAPISpec{
				URI: server.URL,
				TLS: &nifiv1alpha1.NiFiAPITLSSpec{CASecretKeyRef: testSecretKeyRef("nifi-api", "ca.crt")},
				Auth: &nifiv1alpha1.NiFiAPIAuthSpec{
					BearerTokenSecretKeyRef: testSecretKeyRef("nifi-api", "token"),
				},
			},
		},
	}

	if err := configureClusterHTTPClient(t.Context(), newIdentityCanvasTestClient(testScheme(), secret), cluster); err != nil {
		t.Fatal(err)
	}
	if err := (nifi.HTTPReachabilityChecker{}).CheckReachable(t.Context(), server.URL, time.Second); err != nil {
		t.Fatal(err)
	}
}
