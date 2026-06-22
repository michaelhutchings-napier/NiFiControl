package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/certmanager"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func certManagerTestScheme() *runtime.Scheme {
	scheme := managedClusterTestScheme()
	for _, gvk := range []schema.GroupVersionKind{certmanager.CertificateGVK, certmanager.IssuerGVK, certmanager.ClusterIssuerGVK} {
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		listGVK := gvk
		listGVK.Kind += "List"
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	return scheme
}

func newTLSCluster() *nifiv1alpha1.NiFiCluster {
	storageEnabled := false
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:        nifiv1alpha1.ClusterModeInternal,
			Replicas:    1,
			Storage:     nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storageEnabled},
			InternalTLS: &nifiv1alpha1.NiFiClusterInternalTLSSpec{Enabled: true},
		},
	}
}

func getUnstructured(t *testing.T, c client.Client, gvk schema.GroupVersionKind, name, namespace string) *unstructured.Unstructured {
	t.Helper()
	obj := certmanager.New(gvk)
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, obj); err != nil {
		t.Fatalf("get %s %s: %v", gvk.Kind, name, err)
	}
	return obj
}

func issueTLSSecret(t *testing.T, c client.Client, name, namespace string) {
	t.Helper()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	_, err := ctrl.CreateOrUpdate(context.Background(), c, secret, func() error {
		secret.Data = map[string][]byte{
			tlsKeystoreKey:   []byte("keystore-bytes"),
			tlsTruststoreKey: []byte("truststore-bytes"),
			tlsCAKey:         []byte("ca-pem"),
			tlsCertKey:       []byte("cert-pem"),
			tlsKeyKey:        []byte("key-pem"),
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReconcileManagedClusterTLSSelfSignedPendingThenReady(t *testing.T) {
	scheme := certManagerTestScheme()
	cluster := newTLSCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ReachabilityChecker: fakeReachabilityChecker{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}
	plan := resolveTLSPlan(cluster)

	// First pass: provision cert-manager resources, but TLS is pending until materials exist.
	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	getUnstructured(t, k8sClient, certmanager.IssuerGVK, plan.selfSignedIssuerName, cluster.Namespace)
	getUnstructured(t, k8sClient, certmanager.IssuerGVK, plan.caIssuerName, cluster.Namespace)
	getUnstructured(t, k8sClient, certmanager.CertificateGVK, plan.caCertName, cluster.Namespace)
	serverCert := getUnstructured(t, k8sClient, certmanager.CertificateGVK, plan.serverCertName, cluster.Namespace)
	usages, _, _ := unstructured.NestedStringSlice(serverCert.Object, "spec", "usages")
	if !containsString(usages, certmanager.UsageServerAuth) || !containsString(usages, certmanager.UsageClientAuth) {
		t.Fatalf("server certificate usages = %v, want server+client auth", usages)
	}
	dnsNames, _, _ := unstructured.NestedStringSlice(serverCert.Object, "spec", "dnsNames")
	if !containsString(dnsNames, "*.production-nifi-headless.default.svc") {
		t.Fatalf("server certificate is missing the wildcard headless SAN: %v", dnsNames)
	}
	getUnstructured(t, k8sClient, certmanager.CertificateGVK, plan.clientCertName, cluster.Namespace)

	password := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: plan.passwordSecretName, Namespace: cluster.Namespace}, password); err != nil {
		t.Fatal(err)
	}
	if len(password.Data[keystorePasswordKey]) == 0 {
		t.Fatal("keystore password secret was not generated")
	}
	configMap := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: plan.configMapName, Namespace: cluster.Namespace}, configMap); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(configMap.Data["authorizers.xml"], "CN=production-operator") {
		t.Fatalf("authorizers.xml missing operator admin identity: %s", configMap.Data["authorizers.xml"])
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionTLSReady, metav1.ConditionFalse, "TLSPending")

	statefulSet := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, statefulSet); !apierrors.IsNotFound(err) {
		t.Fatalf("StatefulSet should not exist before TLS is ready, got err=%v", err)
	}

	// cert-manager issues the materials.
	issueTLSSecret(t, k8sClient, plan.serverSecretName, cluster.Namespace)
	issueTLSSecret(t, k8sClient, plan.clientSecretName, cluster.Namespace)

	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, statefulSet); err != nil {
		t.Fatalf("StatefulSet should exist once TLS is ready: %v", err)
	}
	container := statefulSet.Spec.Template.Spec.Containers[0]
	if len(container.Command) != 3 || !strings.Contains(container.Command[2], "nifi.web.https.port") || !strings.Contains(container.Command[2], "managed-authorizer") {
		t.Fatalf("StatefulSet is not using the TLS start command: %#v", container.Command)
	}
	if container.ReadinessProbe == nil || container.ReadinessProbe.Exec == nil {
		t.Fatal("expected an exec mTLS readiness probe")
	}
	if container.Ports[0].ContainerPort != 8443 {
		t.Fatalf("web container port = %d, want 8443", container.Ports[0].ContainerPort)
	}
	assertEnvironmentSecretRef(t, container.Env, "NIFI_KEYSTORE_PASSWORD", plan.passwordSecretName)
	if statefulSet.Spec.Template.Annotations[managedTLSChecksumAnnotation] == "" {
		t.Fatal("expected a TLS checksum annotation to roll pods on rotation")
	}
	assertVolumePresent(t, statefulSet.Spec.Template.Spec.Volumes, managedTLSVolume)
	assertVolumePresent(t, statefulSet.Spec.Template.Spec.Volumes, managedTLSConfigVol)

	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.TLS == nil || !current.Status.TLS.Ready {
		t.Fatalf("TLS status = %#v, want ready", current.Status.TLS)
	}
	if current.Status.TLS.InitialAdminIdentity != "CN=production-operator" {
		t.Fatalf("initial admin identity = %q", current.Status.TLS.InitialAdminIdentity)
	}
	if current.Status.Endpoint != "https://production-nifi.default.svc:8443" {
		t.Fatalf("endpoint = %q, want https on 8443", current.Status.Endpoint)
	}
}

func TestReconcileManagedClusterTLSExternal(t *testing.T) {
	scheme := certManagerTestScheme()
	storageEnabled := false
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "edge", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 1,
			Storage:  nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storageEnabled},
			InternalTLS: &nifiv1alpha1.NiFiClusterInternalTLSSpec{
				Enabled: true,
				External: &nifiv1alpha1.NiFiExternalTLSSpec{
					ServerSecretName:          "edge-server",
					ClientSecretName:          "edge-client",
					KeystorePasswordSecretRef: &nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "edge-pw"}, Key: "password"}},
					InitialAdminIdentity:      "CN=edge-admin,O=NiFiControl",
					NodeIdentity:              "CN=edge-node,O=NiFiControl",
				},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ReachabilityChecker: fakeReachabilityChecker{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	issueTLSSecret(t, k8sClient, "edge-server", cluster.Namespace)
	issueTLSSecret(t, k8sClient, "edge-client", cluster.Namespace)

	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	statefulSet := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, statefulSet); err != nil {
		t.Fatalf("external TLS StatefulSet should exist: %v", err)
	}
	assertVolumeSecret(t, statefulSet.Spec.Template.Spec.Volumes, managedTLSVolume, "edge-server")
	assertEnvironmentSecretRef(t, statefulSet.Spec.Template.Spec.Containers[0].Env, "NIFI_KEYSTORE_PASSWORD", "edge-pw")

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.TLS == nil || current.Status.TLS.Mode != "External" || current.Status.TLS.NodeIdentity != "CN=edge-node,O=NiFiControl" {
		t.Fatalf("external TLS status = %#v", current.Status.TLS)
	}
}

func TestReconcileManagedClusterTLSReportsMissingCertManager(t *testing.T) {
	// Intercept cert-manager writes with a no-REST-mapping error, simulating a cluster
	// where the cert-manager CRDs are not installed.
	scheme := certManagerTestScheme()
	cluster := newTLSCluster()
	noCertManager := func(obj client.Object) error {
		if obj.GetObjectKind().GroupVersionKind().Group == certmanager.GroupName {
			return &meta.NoKindMatchError{GroupKind: obj.GetObjectKind().GroupVersionKind().GroupKind()}
		}
		return nil
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := noCertManager(obj); err != nil {
					return err
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ReachabilityChecker: fakeReachabilityChecker{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	for range 2 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionTLSReady, metav1.ConditionFalse, "CertManagerMissing")
	statefulSet := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, statefulSet); !apierrors.IsNotFound(err) {
		t.Fatalf("StatefulSet should not exist when cert-manager is missing, err=%v", err)
	}
}

func TestManagedClusterTLSRotationChangesChecksum(t *testing.T) {
	scheme := certManagerTestScheme()
	cluster := newTLSCluster()
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

	// Rotate the server material.
	rotated := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: plan.serverSecretName, Namespace: cluster.Namespace}, rotated); err != nil {
		t.Fatal(err)
	}
	rotated.Data[tlsKeystoreKey] = []byte("rotated-keystore-bytes")
	if err := k8sClient.Update(context.Background(), rotated); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(context.Background(), key, statefulSet); err != nil {
		t.Fatal(err)
	}
	after := statefulSet.Spec.Template.Annotations[managedTLSChecksumAnnotation]
	if after == before {
		t.Fatalf("checksum did not change after rotation: %s", after)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func assertEnvironmentSecretRef(t *testing.T, environment []corev1.EnvVar, name, secretName string) {
	t.Helper()
	for _, variable := range environment {
		if variable.Name != name {
			continue
		}
		if variable.ValueFrom == nil || variable.ValueFrom.SecretKeyRef == nil || variable.ValueFrom.SecretKeyRef.Name != secretName {
			t.Fatalf("env %s should reference secret %q, got %#v", name, secretName, variable.ValueFrom)
		}
		return
	}
	t.Fatalf("env %s not found", name)
}

func assertVolumePresent(t *testing.T, volumes []corev1.Volume, name string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return
		}
	}
	t.Fatalf("volume %s not found", name)
}

func assertVolumeSecret(t *testing.T, volumes []corev1.Volume, name, secretName string) {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name != name {
			continue
		}
		if volume.Secret == nil || volume.Secret.SecretName != secretName {
			t.Fatalf("volume %s should mount secret %q, got %#v", name, secretName, volume.VolumeSource)
		}
		return
	}
	t.Fatalf("volume %s not found", name)
}
