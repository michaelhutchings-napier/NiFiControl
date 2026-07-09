package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func cloneTLSData(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

// TestTLSChecksumAutoReloadSkipsLeafRotation pins the roll/no-roll decision that makes
// auto-reload meaningful: with it on, leaf rotation must not change the checksum (no pod
// roll), but a CA change or a config change must.
func TestTLSChecksumAutoReloadSkipsLeafRotation(t *testing.T) {
	base := map[string][]byte{
		tlsKeystoreKey:   []byte("ks1"),
		tlsTruststoreKey: []byte("ts1"),
		tlsCAKey:         []byte("ca1"),
		tlsCertKey:       []byte("leaf1"),
		tlsKeyKey:        []byte("key1"),
	}
	const cfg = "config-input"

	// cert-manager rewrites the whole Secret on renewal: the keystore, truststore, and leaf
	// PEM/key all change (PKCS12 is non-deterministic), while ca.crt stays put under the same CA.
	leafRotated := cloneTLSData(base)
	leafRotated[tlsKeystoreKey] = []byte("ks2")
	leafRotated[tlsTruststoreKey] = []byte("ts2")
	leafRotated[tlsCertKey] = []byte("leaf2")
	leafRotated[tlsKeyKey] = []byte("key2")

	// Auto-reload OFF: leaf rotation rolls the pods (checksum changes) — the pre-existing behaviour.
	if tlsChecksum(base, cfg, false) == tlsChecksum(leafRotated, cfg, false) {
		t.Fatal("auto-reload off: leaf rotation must change the checksum")
	}

	// Auto-reload ON: leaf rotation does not roll (checksum stable).
	on := tlsChecksum(base, cfg, true)
	if tlsChecksum(leafRotated, cfg, true) != on {
		t.Fatal("auto-reload on: leaf rotation must NOT change the checksum")
	}

	// Auto-reload ON: a CA rotation is a trust change and still rolls.
	caRotated := cloneTLSData(base)
	caRotated[tlsCAKey] = []byte("ca2")
	if tlsChecksum(caRotated, cfg, true) == on {
		t.Fatal("auto-reload on: CA rotation must change the checksum")
	}

	// Auto-reload ON: operator config changes (authorizers, proxy hosts) still roll.
	if tlsChecksum(base, "config-changed", true) == on {
		t.Fatal("auto-reload on: config change must change the checksum")
	}
}

func TestManagedClusterTLSAutoReloadDefaults(t *testing.T) {
	cluster := newTLSCluster()
	if managedClusterTLSAutoReloadEnabled(cluster) {
		t.Fatal("auto-reload should default off")
	}
	cluster.Spec.InternalTLS.AutoReload = &nifiv1alpha1.NiFiClusterTLSAutoReload{Enabled: true}
	if !managedClusterTLSAutoReloadEnabled(cluster) {
		t.Fatal("expected auto-reload enabled")
	}
	if got := managedClusterTLSAutoReloadInterval(cluster); got != "10 secs" {
		t.Fatalf("expected default interval '10 secs', got %q", got)
	}
	cluster.Spec.InternalTLS.AutoReload.Interval = "30 secs"
	if got := managedClusterTLSAutoReloadInterval(cluster); got != "30 secs" {
		t.Fatalf("expected custom interval '30 secs', got %q", got)
	}
	// Auto-reload is a no-op unless internalTLS itself is enabled.
	cluster.Spec.InternalTLS.Enabled = false
	if managedClusterTLSAutoReloadEnabled(cluster) {
		t.Fatal("auto-reload must require internalTLS.enabled")
	}
}

func TestManagedClusterTLSAutoReloadEnvironment(t *testing.T) {
	cluster := newTLSCluster()
	cluster.Spec.InternalTLS.AutoReload = &nifiv1alpha1.NiFiClusterTLSAutoReload{Enabled: true, Interval: "15 secs"}
	tls := &clusterTLSMaterials{httpsPort: 8443, passwordSecretName: "pw", passwordSecretKey: "password"}

	env := nodeEnvironment(cluster, tls, "512m", "1g", nil, false, nil)
	if v, _ := envValue(env, "NIFI_TLS_AUTORELOAD_ENABLED"); v != "true" {
		t.Fatalf("expected NIFI_TLS_AUTORELOAD_ENABLED=true, got %q", v)
	}
	if v, _ := envValue(env, "NIFI_TLS_AUTORELOAD_INTERVAL"); v != "15 secs" {
		t.Fatalf("expected NIFI_TLS_AUTORELOAD_INTERVAL='15 secs', got %q", v)
	}

	// Disabled: the env is omitted and the start-script defaults leave auto-reload off.
	cluster.Spec.InternalTLS.AutoReload = nil
	env = nodeEnvironment(cluster, tls, "512m", "1g", nil, false, nil)
	if _, ok := envValue(env, "NIFI_TLS_AUTORELOAD_ENABLED"); ok {
		t.Fatal("NIFI_TLS_AUTORELOAD_ENABLED should be absent when auto-reload is disabled")
	}
}

func TestManagedStartCommandRendersAutoReloadProps(t *testing.T) {
	for _, want := range []string{
		`prop_replace 'nifi.security.autoreload.enabled' "${NIFI_TLS_AUTORELOAD_ENABLED:-false}"`,
		`prop_replace 'nifi.security.autoreload.interval' "${NIFI_TLS_AUTORELOAD_INTERVAL:-10 secs}"`,
	} {
		if !strings.Contains(managedNiFiStartCommandTLS, want) {
			t.Fatalf("TLS start command missing auto-reload property: %s", want)
		}
	}
}

// TestManagedClusterTLSAutoReloadNoRollOnLeafRotation is the end-to-end operator proof: with
// auto-reload on, a leaf rotation leaves the StatefulSet's tls-checksum annotation unchanged
// (no roll), while a CA rotation changes it.
func TestManagedClusterTLSAutoReloadNoRollOnLeafRotation(t *testing.T) {
	scheme := certManagerTestScheme()
	cluster := newTLSCluster()
	cluster.Spec.InternalTLS.AutoReload = &nifiv1alpha1.NiFiClusterTLSAutoReload{Enabled: true}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ReachabilityChecker: fakeReachabilityChecker{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}
	plan := resolveTLSPlan(cluster)

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	issueTLSSecret(t, k8sClient, plan.serverSecretName, cluster.Namespace)
	issueTLSSecret(t, k8sClient, plan.clientSecretName, cluster.Namespace)
	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	statefulSet := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, statefulSet); err != nil {
		t.Fatal(err)
	}
	before := statefulSet.Spec.Template.Annotations[managedTLSChecksumAnnotation]
	if before == "" {
		t.Fatal("missing checksum before rotation")
	}

	// Leaf rotation: keystore/truststore/leaf change, ca.crt stays. Must NOT roll.
	rotateServer := func(mutate func(map[string][]byte)) {
		t.Helper()
		secret := &corev1.Secret{}
		if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: plan.serverSecretName, Namespace: cluster.Namespace}, secret); err != nil {
			t.Fatal(err)
		}
		mutate(secret.Data)
		if err := k8sClient.Update(context.Background(), secret); err != nil {
			t.Fatal(err)
		}
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		if err := k8sClient.Get(context.Background(), key, statefulSet); err != nil {
			t.Fatal(err)
		}
	}

	rotateServer(func(d map[string][]byte) {
		d[tlsKeystoreKey] = []byte("rotated-keystore")
		d[tlsTruststoreKey] = []byte("rotated-truststore")
		d[tlsCertKey] = []byte("rotated-leaf")
		d[tlsKeyKey] = []byte("rotated-key")
	})
	if got := statefulSet.Spec.Template.Annotations[managedTLSChecksumAnnotation]; got != before {
		t.Fatalf("leaf rotation rolled the pods with auto-reload on: %s -> %s", before, got)
	}

	// CA rotation: ca.crt changes. Must roll.
	rotateServer(func(d map[string][]byte) { d[tlsCAKey] = []byte("rotated-ca-pem") })
	if got := statefulSet.Spec.Template.Annotations[managedTLSChecksumAnnotation]; got == before {
		t.Fatalf("CA rotation did not roll the pods: %s", got)
	}
}
