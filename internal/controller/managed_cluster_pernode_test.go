package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func perNodeTLSCluster() *nifiv1alpha1.NiFiCluster {
	cluster := newTLSCluster()
	cluster.Spec.InternalTLS.PerNodeCertificates = &nifiv1alpha1.NiFiClusterPerNodeCertificates{Enabled: true}
	return cluster
}

func TestManagedClusterPerNodeCertificatesEnabled(t *testing.T) {
	if managedClusterPerNodeCertificatesEnabled(newTLSCluster()) {
		t.Fatal("per-node should default off")
	}
	if !managedClusterPerNodeCertificatesEnabled(perNodeTLSCluster()) {
		t.Fatal("expected per-node enabled")
	}
	// Not applicable to external TLS.
	external := perNodeTLSCluster()
	external.Spec.InternalTLS.External = &nifiv1alpha1.NiFiExternalTLSSpec{ServerSecretName: "s", ClientSecretName: "c"}
	if managedClusterPerNodeCertificatesEnabled(external) {
		t.Fatal("per-node must be disabled with external TLS")
	}
}

func TestResolveTLSPlanPerNodeNodeIdentity(t *testing.T) {
	plan := resolveTLSPlan(perNodeTLSCluster())
	if plan.nodeIdentity != perNodeNodeAuthzIdentity {
		t.Fatalf("expected node identity %q, got %q", perNodeNodeAuthzIdentity, plan.nodeIdentity)
	}
	// The rendered authorizers seed the node policies with the mapped identity, not a per-pod CN.
	authorizers := renderAuthorizersXML(plan.initialAdminIdentity, plan.nodeIdentity)
	if !strings.Contains(authorizers, `<property name="Node Identity node">node</property>`) {
		t.Fatalf("authorizers.xml should use the mapped node identity:\n%s", authorizers)
	}
}

func TestManagedClusterCSIDNSNames(t *testing.T) {
	names := managedClusterCSIDNSNames(newTLSCluster())
	for _, want := range []string{
		"${POD_NAME}.production-nifi-headless.${POD_NAMESPACE}.svc",
		"production-nifi.${POD_NAMESPACE}.svc",
		"localhost",
	} {
		if !strings.Contains(names, want) {
			t.Fatalf("dns-names %q missing %q", names, want)
		}
	}
}

func TestNodeVolumesEmitsCSIWhenPerNode(t *testing.T) {
	tls := &clusterTLSMaterials{
		perNode:       true,
		configMapName: "cfg",
		csi: csiCertParams{
			issuerName: "ca-issuer", issuerKind: "Issuer", issuerGroup: "cert-manager.io",
			commonName: "node-${POD_NAME}", dnsNames: "localhost", keyUsages: "server auth,client auth",
		},
	}
	volumes := nodeVolumes(true, tls, "", "")
	var found *corev1.Volume
	for i := range volumes {
		if volumes[i].Name == managedTLSVolume {
			found = &volumes[i]
		}
	}
	if found == nil || found.CSI == nil {
		t.Fatalf("expected a CSI TLS volume, got %#v", found)
	}
	if found.CSI.Driver != certManagerCSIDriverName {
		t.Fatalf("wrong CSI driver: %s", found.CSI.Driver)
	}
	attrs := found.CSI.VolumeAttributes
	if attrs["csi.cert-manager.io/issuer-name"] != "ca-issuer" ||
		attrs["csi.cert-manager.io/common-name"] != "node-${POD_NAME}" ||
		attrs["csi.cert-manager.io/fs-group"] != "1000" {
		t.Fatalf("unexpected CSI attributes: %#v", attrs)
	}
	if found.Secret != nil {
		t.Fatal("per-node TLS volume must not be a Secret volume")
	}

	// Shared mode keeps the Secret volume.
	shared := nodeVolumes(true, &clusterTLSMaterials{serverSecretName: "srv", configMapName: "cfg"}, "", "")
	for i := range shared {
		if shared[i].Name == managedTLSVolume {
			if shared[i].Secret == nil || shared[i].CSI != nil {
				t.Fatal("shared mode must use a Secret volume")
			}
		}
	}
}

func TestStartCommandPerNodeBuildsKeystoreAndMapsIdentity(t *testing.T) {
	for _, want := range []string{
		`if [ "${NIFI_TLS_PER_NODE:-false}" = "true" ]; then`,
		`openssl pkcs12 -export -name nifi-node`,
		// The mapping keys are appended (prop_replace only rewrites existing lines, and these keys
		// are absent from the stock nifi.properties), guarded so a persisted conf is not duplicated.
		`grep -q '^nifi.security.identity.mapping.pattern.nificontrolnode=' "${nifi_props_file}"`,
		`'nifi.security.identity.mapping.pattern.nificontrolnode=^CN=node-.*$'`,
		`'nifi.security.identity.mapping.value.nificontrolnode=node'`,
		`'nifi.security.identity.mapping.transform.nificontrolnode=NONE'`,
		`>> "${nifi_props_file}"`,
		`prop_replace 'nifi.security.keystore' "${nificontrol_keystore}"`,
	} {
		if !strings.Contains(managedNiFiStartCommandTLS, want) {
			t.Fatalf("TLS start command missing per-node fragment: %s", want)
		}
	}
}

func TestNodeEnvironmentSetsPerNodeFlag(t *testing.T) {
	cluster := perNodeTLSCluster()
	tls := &clusterTLSMaterials{httpsPort: 8443, perNode: true, passwordSecretName: "pw", passwordSecretKey: "password"}
	env := nodeEnvironment(cluster, tls, "512m", "1g", nil, false, nil)
	if v, _ := envValue(env, "NIFI_TLS_PER_NODE"); v != "true" {
		t.Fatalf("expected NIFI_TLS_PER_NODE=true, got %q", v)
	}
	// Shared mode does not set it.
	env = nodeEnvironment(cluster, &clusterTLSMaterials{httpsPort: 8443, passwordSecretName: "pw", passwordSecretKey: "password"}, "512m", "1g", nil, false, nil)
	if _, ok := envValue(env, "NIFI_TLS_PER_NODE"); ok {
		t.Fatal("shared mode must not set NIFI_TLS_PER_NODE")
	}
}

func TestReconcileManagedClusterTLSPerNodeRequiresCSIDriver(t *testing.T) {
	scheme := certManagerTestScheme()
	cluster := perNodeTLSCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	materials, ready, err := reconciler.reconcileManagedClusterTLS(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if ready || materials != nil {
		t.Fatal("expected TLS not ready without the CSI driver")
	}
	fresh := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, fresh); err != nil {
		t.Fatal(err)
	}
	if !conditionMatches(fresh.Status.Conditions, nifiv1alpha1.ConditionTLSReady, metav1.ConditionFalse, "CSIDriverMissing") {
		t.Fatalf("expected CSIDriverMissing condition, got %+v", fresh.Status.Conditions)
	}
}

func TestReconcileManagedClusterTLSPerNodeReady(t *testing.T) {
	scheme := certManagerTestScheme()
	cluster := perNodeTLSCluster()
	csiDriver := &storagev1.CSIDriver{ObjectMeta: metav1.ObjectMeta{Name: certManagerCSIDriverName}}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, csiDriver).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	plan := resolveTLSPlan(cluster)

	// First pass provisions the issuers and the operator client Certificate.
	if _, _, err := reconciler.reconcileManagedClusterTLS(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	// Only the operator CLIENT Secret is needed for per-node (no shared server Secret).
	issueTLSSecret(t, k8sClient, plan.clientSecretName, cluster.Namespace)

	materials, ready, err := reconciler.reconcileManagedClusterTLS(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !ready || materials == nil {
		t.Fatal("expected TLS ready with the CSI driver and client Secret present")
	}
	if !materials.perNode {
		t.Fatal("materials should be marked per-node")
	}
	if materials.csi.issuerName == "" || materials.csi.commonName != "node-${POD_NAME}" {
		t.Fatalf("unexpected CSI params: %#v", materials.csi)
	}
	if materials.nodeIdentity != perNodeNodeAuthzIdentity {
		t.Fatalf("expected mapped node identity, got %q", materials.nodeIdentity)
	}
	// No shared server Secret should have been created for a per-node cluster.
	serverSecret := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: plan.serverSecretName, Namespace: cluster.Namespace}, serverSecret); err == nil {
		t.Fatal("per-node clusters must not create a shared server Secret")
	}
}
