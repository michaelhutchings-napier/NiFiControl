package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNiFiUserReconcileWaitsForCluster(t *testing.T) {
	scheme := testScheme()
	user := &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiUserSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "missing"},
			Identity:   "alice@example.com",
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, user)
	reconciler := &NiFiUserReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace}}

	reconcileUserTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiUser{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "DependenciesNotReady")
}

func TestNiFiUserGroupReconcileWaitsForMemberUser(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	userGroup := &nifiv1alpha1.NiFiUserGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "operators", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiUserGroupSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Identity:   "operators",
			Users: []nifiv1alpha1.UserGroupMember{
				{UserRef: nifiv1alpha1.LocalObjectReference{Name: "alice"}},
			},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, userGroup)
	reconciler := &NiFiUserGroupReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: userGroup.Name, Namespace: userGroup.Namespace}}

	reconcileUserGroupTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiUserGroup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	if len(current.Status.Dependencies.WaitingFor) != 1 || current.Status.Dependencies.WaitingFor[0] != "NiFiUser/default/alice" {
		t.Fatalf("waitingFor = %#v, want missing alice user", current.Status.Dependencies.WaitingFor)
	}
}

func TestNiFiProcessGroupReconcileWaitsForParameterContext(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	processGroup := &nifiv1alpha1.NiFiProcessGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessGroupSpec{
			ClusterRef:          nifiv1alpha1.ClusterReference{Name: cluster.Name},
			DisplayName:         "Payments",
			ParameterContextRef: &nifiv1alpha1.LocalObjectReference{Name: "payments-prod"},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, processGroup)
	reconciler := &NiFiProcessGroupReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: processGroup.Name, Namespace: processGroup.Namespace}}

	reconcileProcessGroupTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiProcessGroup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	if len(current.Status.Dependencies.WaitingFor) != 1 || current.Status.Dependencies.WaitingFor[0] != "NiFiParameterContext/default/payments-prod" {
		t.Fatalf("waitingFor = %#v, want missing parameter context", current.Status.Dependencies.WaitingFor)
	}
}

func newIdentityCanvasTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiUser{}, &nifiv1alpha1.NiFiUserGroup{}, &nifiv1alpha1.NiFiProcessGroup{}).
		Build()
}

func reconcileUserTwice(t *testing.T, reconciler *NiFiUserReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileUserGroupTwice(t *testing.T, reconciler *NiFiUserGroupReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileProcessGroupTwice(t *testing.T, reconciler *NiFiProcessGroupReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}
