package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/keda"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func kedaTestScheme() *runtime.Scheme {
	scheme := managedClusterTestScheme()
	gvk := keda.ScaledObjectGVK
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	listGVK := gvk
	listGVK.Kind += "List"
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return scheme
}

func newAutoscalerCluster() *nifiv1alpha1.NiFiCluster {
	storageEnabled := false
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 3,
			Storage:  nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storageEnabled},
		},
		Status: nifiv1alpha1.NiFiClusterStatus{Replicas: 3},
	}
}

func newAutoscaler(metrics []nifiv1alpha1.NiFiAutoscalerMetric) *nifiv1alpha1.NiFiAutoscaler {
	return &nifiv1alpha1.NiFiAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "flow-as", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiAutoscalerSpec{
			Target:      nifiv1alpha1.NiFiAutoscalerTarget{Kind: "NiFiCluster", Name: "production"},
			MinReplicas: 3,
			MaxReplicas: 9,
			Metrics:     metrics,
		},
	}
}

func prometheusMetric() nifiv1alpha1.NiFiAutoscalerMetric {
	return nifiv1alpha1.NiFiAutoscalerMetric{
		Type: "Prometheus",
		Prometheus: &nifiv1alpha1.PrometheusMetricSource{
			ServerAddress: "http://prometheus.monitoring.svc:9090",
			Query:         "sum(nifi_amount_items_queued)",
			Threshold:     "10000",
		},
	}
}

func reconcileAutoscaler(t *testing.T, c client.Client, scheme *runtime.Scheme, name string) {
	t.Helper()
	r := &NiFiAutoscalerReconciler{Client: c, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func getAutoscaler(t *testing.T, c client.Client, name string) *nifiv1alpha1.NiFiAutoscaler {
	t.Helper()
	got := &nifiv1alpha1.NiFiAutoscaler{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, got); err != nil {
		t.Fatalf("get autoscaler: %v", err)
	}
	return got
}

func TestNiFiAutoscalerRendersKEDAScaledObject(t *testing.T) {
	scheme := kedaTestScheme()
	cluster := newAutoscalerCluster()
	as := newAutoscaler([]nifiv1alpha1.NiFiAutoscalerMetric{prometheusMetric()})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, as).
		WithStatusSubresource(&nifiv1alpha1.NiFiAutoscaler{}).Build()
	reconcileAutoscaler(t, c, scheme, as.Name)

	so := keda.New()
	if err := c.Get(context.Background(), types.NamespacedName{Name: autoscalerResourceName(as), Namespace: "default"}, so); err != nil {
		t.Fatalf("ScaledObject not created: %v", err)
	}
	kind, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "kind")
	if kind != "NiFiCluster" {
		t.Errorf("scaleTargetRef.kind = %q, want NiFiCluster", kind)
	}
	maxReplicas, _, _ := unstructured.NestedInt64(so.Object, "spec", "maxReplicaCount")
	if maxReplicas != 9 {
		t.Errorf("maxReplicaCount = %d, want 9", maxReplicas)
	}
	triggers, _, _ := unstructured.NestedSlice(so.Object, "spec", "triggers")
	if len(triggers) != 1 {
		t.Fatalf("want 1 trigger, got %d", len(triggers))
	}
	trigger := triggers[0].(map[string]any)
	if trigger["type"] != "prometheus" {
		t.Errorf("trigger type = %v", trigger["type"])
	}
	meta, _, _ := unstructured.NestedStringMap(trigger, "metadata")
	if meta["query"] != "sum(nifi_amount_items_queued)" || meta["threshold"] != "10000" {
		t.Errorf("trigger metadata = %v", meta)
	}
	// NiFi-safe scale-down: one node at a time.
	policies, _, _ := unstructured.NestedSlice(so.Object, "spec", "advanced", "horizontalPodAutoscalerConfig", "behavior", "scaleDown", "policies")
	if len(policies) != 1 || policies[0].(map[string]any)["value"].(int64) != 1 {
		t.Errorf("expected one Pods=1 scale-down policy, got %v", policies)
	}

	got := getAutoscaler(t, c, as.Name)
	if !got.Status.Ready || got.Status.Mode != "KEDA" || got.Status.ScaledObjectName == "" {
		t.Fatalf("status not KEDA-ready: %+v", got.Status)
	}
}

func TestNiFiAutoscalerRendersHPAForResourceMetric(t *testing.T) {
	scheme := kedaTestScheme()
	cluster := newAutoscalerCluster()
	as := newAutoscaler([]nifiv1alpha1.NiFiAutoscalerMetric{{
		Type:     "Resource",
		Resource: &nifiv1alpha1.ResourceMetricSource{Name: "cpu", TargetAverageUtilization: 70},
	}})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, as).
		WithStatusSubresource(&nifiv1alpha1.NiFiAutoscaler{}).Build()
	reconcileAutoscaler(t, c, scheme, as.Name)

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: autoscalerResourceName(as), Namespace: "default"}, hpa); err != nil {
		t.Fatalf("HPA not created: %v", err)
	}
	if hpa.Spec.ScaleTargetRef.Kind != "NiFiCluster" || hpa.Spec.MaxReplicas != 9 {
		t.Errorf("unexpected scaleTargetRef/max: %+v", hpa.Spec)
	}
	if len(hpa.Spec.Metrics) != 1 || hpa.Spec.Metrics[0].Resource == nil || hpa.Spec.Metrics[0].Resource.Name != "cpu" {
		t.Fatalf("expected one cpu resource metric, got %+v", hpa.Spec.Metrics)
	}
	if hpa.Spec.Behavior == nil || hpa.Spec.Behavior.ScaleDown == nil || len(hpa.Spec.Behavior.ScaleDown.Policies) != 1 {
		t.Fatalf("expected a scale-down policy, got %+v", hpa.Spec.Behavior)
	}
	got := getAutoscaler(t, c, as.Name)
	if !got.Status.Ready || got.Status.Mode != "HPA" || got.Status.HPAName == "" {
		t.Fatalf("status not HPA-ready: %+v", got.Status)
	}
}

func TestNiFiAutoscalerReportsMissingKEDA(t *testing.T) {
	scheme := kedaTestScheme()
	cluster := newAutoscalerCluster()
	as := newAutoscaler([]nifiv1alpha1.NiFiAutoscalerMetric{prometheusMetric()})
	noKEDA := func(obj client.Object) error {
		if obj.GetObjectKind().GroupVersionKind().Group == keda.GroupName {
			return &meta.NoKindMatchError{GroupKind: obj.GetObjectKind().GroupVersionKind().GroupKind()}
		}
		return nil
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, as).
		WithStatusSubresource(&nifiv1alpha1.NiFiAutoscaler{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := noKEDA(obj); err != nil {
					return err
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	reconcileAutoscaler(t, c, scheme, as.Name)

	got := getAutoscaler(t, c, as.Name)
	if got.Status.Ready {
		t.Fatal("autoscaler should not be Ready when KEDA is missing")
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "KEDANotInstalled")
}

func TestNiFiAutoscalerAcceptsNonPrimary(t *testing.T) {
	scheme := kedaTestScheme()
	cluster := newAutoscalerCluster()
	as := newAutoscaler([]nifiv1alpha1.NiFiAutoscalerMetric{prometheusMetric()})
	as.Spec.Behavior = &nifiv1alpha1.NiFiAutoscalerBehavior{ScaleDownStrategy: nifiv1alpha1.ScaleDownNonPrimary}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, as).
		WithStatusSubresource(&nifiv1alpha1.NiFiAutoscaler{}).Build()
	reconcileAutoscaler(t, c, scheme, as.Name)

	got := getAutoscaler(t, c, as.Name)
	if !got.Status.Ready || got.Status.Mode != "KEDA" {
		t.Fatalf("NonPrimary strategy should reconcile Ready in KEDA mode: %+v", got.Status)
	}
	// The ScaledObject is rendered normally: NonPrimary maps to the same highest-ordinal offload.
	so := keda.New()
	if err := c.Get(context.Background(), types.NamespacedName{Name: autoscalerResourceName(as), Namespace: "default"}, so); err != nil {
		t.Fatalf("ScaledObject should exist for the NonPrimary strategy: %v", err)
	}
}

func TestNiFiAutoscalerTargetNotFound(t *testing.T) {
	scheme := kedaTestScheme()
	as := newAutoscaler([]nifiv1alpha1.NiFiAutoscalerMetric{prometheusMetric()})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(as).
		WithStatusSubresource(&nifiv1alpha1.NiFiAutoscaler{}).Build()
	reconcileAutoscaler(t, c, scheme, as.Name)

	got := getAutoscaler(t, c, as.Name)
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "TargetNotFound")
}
