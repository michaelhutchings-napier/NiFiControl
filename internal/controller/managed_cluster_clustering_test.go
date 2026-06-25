package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileSensitivePropsKeySecretIsStable(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(3)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	ctx := context.Background()

	if err := r.reconcileSensitivePropsKeySecret(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: managedClusterSensitivePropsSecretName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	first := secret.Data[sensitivePropsKeyKey]
	if len(first) < 12 {
		t.Fatalf("sensitive props key too short: %d bytes", len(first))
	}
	if len(secret.OwnerReferences) != 1 {
		t.Fatalf("expected an owner reference, got %#v", secret.OwnerReferences)
	}

	// Re-reconciling must not rotate the key (changing it would orphan encrypted values).
	if err := r.reconcileSensitivePropsKeySecret(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data[sensitivePropsKeyKey]) != string(first) {
		t.Fatal("sensitive props key must be stable across reconciliations")
	}
}

func TestClusteredEnvironmentInjectsSensitivePropsKey(t *testing.T) {
	cluster := scaleDownCluster(3)
	env := managedClusterEnvironment(cluster, nil)
	var found *corev1.EnvVar
	for i := range env {
		if env[i].Name == "NIFI_SENSITIVE_PROPS_KEY" {
			found = &env[i]
		}
	}
	if found == nil {
		t.Fatal("clustered environment must inject NIFI_SENSITIVE_PROPS_KEY")
	}
	if found.ValueFrom == nil || found.ValueFrom.SecretKeyRef == nil ||
		found.ValueFrom.SecretKeyRef.Name != managedClusterSensitivePropsSecretName(cluster) ||
		found.ValueFrom.SecretKeyRef.Key != sensitivePropsKeyKey {
		t.Fatalf("NIFI_SENSITIVE_PROPS_KEY source = %#v", found.ValueFrom)
	}

	// Single-node clusters keep the previous behaviour: no explicit key injected.
	single := scaleDownCluster(1)
	single.Spec.Coordination = nil
	for _, e := range managedClusterEnvironment(single, nil) {
		if e.Name == "NIFI_SENSITIVE_PROPS_KEY" {
			t.Fatal("single-node clusters should not inject NIFI_SENSITIVE_PROPS_KEY")
		}
	}

	// Both start commands apply the key.
	for _, cmd := range []string{managedNiFiStartCommand, managedNiFiStartCommandTLS} {
		if !strings.Contains(cmd, "nifi.sensitive.props.key") {
			t.Fatal("start command must set nifi.sensitive.props.key")
		}
	}
}

func TestClusteredEnvironmentAdvertisesPodHost(t *testing.T) {
	// A clustered node must advertise its routable pod DNS name as the web host, because NiFi
	// reports that as the node's address in /controller/cluster, which the operator matches
	// when offloading on scale-down. A 0.0.0.0 web host would make every node indistinguishable.
	cluster := scaleDownCluster(3)
	wantHost := "$(POD_NAME).production-nifi-headless.$(POD_NAMESPACE).svc"
	assertEnvironmentValue(t, managedClusterEnvironment(cluster, nil), "NIFI_WEB_HTTP_HOST", wantHost)

	single := scaleDownCluster(1)
	single.Spec.Coordination = nil
	assertEnvironmentValue(t, managedClusterEnvironment(single, nil), "NIFI_WEB_HTTP_HOST", "0.0.0.0")

	// The TLS start command honours the advertised host instead of hardcoding 0.0.0.0.
	if !strings.Contains(managedNiFiStartCommandTLS, `"${NIFI_WEB_HTTPS_HOST:-0.0.0.0}"`) {
		t.Fatal("TLS start command must set nifi.web.https.host from NIFI_WEB_HTTPS_HOST")
	}
}
