package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeReachabilityChecker struct {
	err error
}

func (f fakeReachabilityChecker) CheckReachable(ctx context.Context, baseURI string, timeout time.Duration) error {
	return f.err
}

func TestNiFiClusterReconcileMarksReachableAPIReady(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))

	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			API: &nifiv1alpha1.NiFiClusterAPISpec{URI: "https://nifi.example.com"},
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		Build()
	reconciler := &NiFiClusterReconciler{
		Client:              client,
		Scheme:              scheme,
		ReachabilityChecker: fakeReachabilityChecker{},
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("cluster ready = false, want true")
	}
	if current.Status.Endpoint != "https://nifi.example.com" {
		t.Fatalf("endpoint = %q, want API URI", current.Status.Endpoint)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionClusterReachable, metav1.ConditionTrue, "ClusterReachable")
}

func TestNiFiClusterReconcileMarksUnreachableAPINotReady(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))

	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			API: &nifiv1alpha1.NiFiClusterAPISpec{URI: "https://nifi.example.com"},
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).
		Build()
	reconciler := &NiFiClusterReconciler{
		Client:              client,
		Scheme:              scheme,
		ReachabilityChecker: fakeReachabilityChecker{err: errors.New("connection refused")},
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := client.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Ready {
		t.Fatal("cluster ready = true, want false")
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionClusterReachable, metav1.ConditionFalse, "ClusterUnreachable")
}

func assertControllerCondition(t *testing.T, conditions []metav1.Condition, conditionType nifiv1alpha1.ConditionType, status metav1.ConditionStatus, reason string) {
	t.Helper()
	for _, condition := range conditions {
		if condition.Type != string(conditionType) {
			continue
		}
		if condition.Status != status {
			t.Fatalf("%s status = %s, want %s", conditionType, condition.Status, status)
		}
		if condition.Reason != reason {
			t.Fatalf("%s reason = %q, want %q", conditionType, condition.Reason, reason)
		}
		return
	}
	t.Fatalf("condition %s not found", conditionType)
}
