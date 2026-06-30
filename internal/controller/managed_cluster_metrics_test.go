package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func serviceMonitorTestScheme() *runtime.Scheme {
	scheme := managedClusterTestScheme()
	gvk := prometheus.ServiceMonitorGVK
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	listGVK := gvk
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return scheme
}

func newMetricsCluster(serviceMonitor bool) *nifiv1alpha1.NiFiCluster {
	storageEnabled := false
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 1,
			Storage:  nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storageEnabled},
			Metrics:  &nifiv1alpha1.NiFiClusterMetricsSpec{Enabled: true},
		},
	}
	if serviceMonitor {
		cluster.Spec.Metrics.ServiceMonitor = &nifiv1alpha1.NiFiClusterServiceMonitorSpec{Enabled: true, Interval: "30s"}
	}
	return cluster
}

func firstEndpoint(t *testing.T, sm *unstructured.Unstructured) map[string]any {
	t.Helper()
	endpoints, found, err := unstructured.NestedSlice(sm.Object, "spec", "endpoints")
	if err != nil || !found || len(endpoints) == 0 {
		t.Fatalf("ServiceMonitor has no endpoints (found=%v err=%v)", found, err)
	}
	endpoint, ok := endpoints[0].(map[string]any)
	if !ok {
		t.Fatalf("endpoint is not a map: %T", endpoints[0])
	}
	return endpoint
}

func TestReconcileManagedClusterMetricsRendersServiceMonitor(t *testing.T) {
	scheme := serviceMonitorTestScheme()
	cluster := newMetricsCluster(true)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterMetrics(context.Background(), cluster, nil); err != nil {
		t.Fatal(err)
	}

	sm := getUnstructured(t, k8sClient, prometheus.ServiceMonitorGVK, managedClusterServiceMonitorName(cluster), cluster.Namespace)
	endpoint := firstEndpoint(t, sm)
	if endpoint["scheme"] != "http" {
		t.Errorf("scheme = %v, want http", endpoint["scheme"])
	}
	if endpoint["port"] != "web" {
		t.Errorf("port = %v, want web", endpoint["port"])
	}
	if endpoint["path"] != defaultMetricsPath {
		t.Errorf("path = %v, want %s", endpoint["path"], defaultMetricsPath)
	}
	if endpoint["interval"] != "30s" {
		t.Errorf("interval = %v, want 30s", endpoint["interval"])
	}
	if _, ok := endpoint["tlsConfig"]; ok {
		t.Error("non-TLS cluster should not render tlsConfig")
	}
	selector, _, _ := unstructured.NestedStringMap(sm.Object, "spec", "selector", "matchLabels")
	if selector[managedClusterMetricsServiceLabel] != "true" {
		t.Errorf("selector missing metrics-service label: %v", selector)
	}
	if selector[managedClusterLabel] != managedClusterResourceName(cluster) {
		t.Errorf("selector missing cluster label: %v", selector)
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Metrics == nil || current.Status.Metrics.ServiceMonitorName != managedClusterServiceMonitorName(cluster) {
		t.Fatalf("status.metrics not populated: %+v", current.Status.Metrics)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionMetricsReady, metav1.ConditionTrue, "ServiceMonitorReady")
}

func TestReconcileManagedClusterMetricsTLSUsesHTTPS(t *testing.T) {
	scheme := serviceMonitorTestScheme()
	cluster := newMetricsCluster(true)
	cluster.Spec.InternalTLS = &nifiv1alpha1.NiFiClusterInternalTLSSpec{Enabled: true}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	tls := &clusterTLSMaterials{clientSecretName: "production-nifi-operator-tls"}

	if err := r.reconcileManagedClusterMetrics(context.Background(), cluster, tls); err != nil {
		t.Fatal(err)
	}

	sm := getUnstructured(t, k8sClient, prometheus.ServiceMonitorGVK, managedClusterServiceMonitorName(cluster), cluster.Namespace)
	endpoint := firstEndpoint(t, sm)
	if endpoint["scheme"] != "https" {
		t.Fatalf("scheme = %v, want https", endpoint["scheme"])
	}
	tlsConfig, ok := endpoint["tlsConfig"].(map[string]any)
	if !ok {
		t.Fatalf("TLS cluster should render tlsConfig, got %T", endpoint["tlsConfig"])
	}
	if tlsConfig["serverName"] != "production-nifi.default.svc" {
		t.Errorf("serverName = %v, want production-nifi.default.svc", tlsConfig["serverName"])
	}
	ca, _, _ := unstructured.NestedStringMap(tlsConfig, "ca", "secret")
	if ca["name"] != "production-nifi-operator-tls" || ca["key"] != "ca.crt" {
		t.Errorf("ca secret = %v, want production-nifi-operator-tls/ca.crt", ca)
	}
	keySecret, _, _ := unstructured.NestedStringMap(tlsConfig, "keySecret")
	if keySecret["name"] != "production-nifi-operator-tls" || keySecret["key"] != "tls.key" {
		t.Errorf("keySecret = %v, want production-nifi-operator-tls/tls.key", keySecret)
	}
}

func TestReconcileManagedClusterMetricsReportsMissingCRD(t *testing.T) {
	scheme := serviceMonitorTestScheme()
	cluster := newMetricsCluster(true)
	noPrometheusOperator := func(obj client.Object) error {
		if obj.GetObjectKind().GroupVersionKind().Group == prometheus.GroupName {
			return &meta.NoKindMatchError{GroupKind: obj.GetObjectKind().GroupVersionKind().GroupKind()}
		}
		return nil
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := noPrometheusOperator(obj); err != nil {
					return err
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterMetrics(context.Background(), cluster, nil); err != nil {
		t.Fatalf("a missing Prometheus Operator must be non-fatal, got: %v", err)
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, current); err != nil {
		t.Fatal(err)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionMetricsReady, metav1.ConditionFalse, "CRDsNotInstalled")

	sm := prometheus.New()
	sm.SetName(managedClusterServiceMonitorName(cluster))
	sm.SetNamespace(cluster.Namespace)
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: sm.GetName(), Namespace: sm.GetNamespace()}, sm); !apierrors.IsNotFound(err) {
		t.Fatalf("ServiceMonitor should not exist when the CRD is missing, err=%v", err)
	}
}

func TestReconcileManagedClusterMetricsDisabledAddsNoCondition(t *testing.T) {
	scheme := serviceMonitorTestScheme()
	storageEnabled := false
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 1,
			Storage:  nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storageEnabled},
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterMetrics(context.Background(), cluster, nil); err != nil {
		t.Fatal(err)
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, current); err != nil {
		t.Fatal(err)
	}
	if conditionPresent(current.Status.Conditions, nifiv1alpha1.ConditionMetricsReady) {
		t.Error("a cluster that never enabled metrics should not get a MetricsReady condition")
	}
}
