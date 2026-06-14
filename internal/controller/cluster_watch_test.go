package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestIndexClusterRefDefaultsNamespace(t *testing.T) {
	ref := nifiv1alpha1.ClusterReference{Name: "production"}

	values := indexClusterRef("default", ref)

	if len(values) != 1 || values[0] != "default/production" {
		t.Fatalf("index values = %#v, want default/production", values)
	}
}

func TestIndexClusterRefUsesExplicitNamespace(t *testing.T) {
	ref := nifiv1alpha1.ClusterReference{Name: "production", Namespace: "platform"}

	values := indexClusterRef("default", ref)

	if len(values) != 1 || values[0] != "platform/production" {
		t.Fatalf("index values = %#v, want platform/production", values)
	}
}

func TestRegistryClientRequestsForClusterUsesClusterRefIndex(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))

	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "platform"},
	}
	matching := &nifiv1alpha1.NiFiRegistryClient{
		ObjectMeta: metav1.ObjectMeta{Name: "matching", Namespace: "apps"},
		Spec: nifiv1alpha1.NiFiRegistryClientSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production", Namespace: "platform"},
		},
	}
	other := &nifiv1alpha1.NiFiRegistryClient{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "apps"},
		Spec: nifiv1alpha1.NiFiRegistryClientSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "other", Namespace: "platform"},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, matching, other).
		WithIndex(&nifiv1alpha1.NiFiRegistryClient{}, clusterRefIndexField, indexRegistryClientClusterRef).
		Build()

	reconciler := &NiFiRegistryClientReconciler{Client: client, Scheme: scheme}
	requests := reconciler.requestsForCluster(context.Background(), cluster)

	if len(requests) != 1 {
		t.Fatalf("requests length = %d, want 1: %#v", len(requests), requests)
	}
	want := ctrl.Request{NamespacedName: types.NamespacedName{Name: matching.Name, Namespace: matching.Namespace}}
	if requests[0] != want {
		t.Fatalf("request = %#v, want %#v", requests[0], want)
	}
}
