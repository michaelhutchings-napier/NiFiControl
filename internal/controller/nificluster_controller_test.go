package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
			Mode: nifiv1alpha1.ClusterModeExternal,
			API:  &nifiv1alpha1.NiFiClusterAPISpec{URI: "https://nifi.example.com"},
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
			Mode: nifiv1alpha1.ClusterModeExternal,
			API:  &nifiv1alpha1.NiFiClusterAPISpec{URI: "https://nifi.example.com"},
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

func TestNiFiClusterReconcileCreatesManagedWorkload(t *testing.T) {
	scheme := managedClusterTestScheme()
	storageEnabled := false
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Image:    "apache/nifi:2.10.0",
			Replicas: 1,
			Storage:  nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storageEnabled},
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ReachabilityChecker: fakeReachabilityChecker{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	statefulSet := &appsv1.StatefulSet{}
	workloadName := managedClusterResourceName(cluster)
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: workloadName, Namespace: cluster.Namespace}, statefulSet); err != nil {
		t.Fatal(err)
	}
	if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 1 {
		t.Fatalf("statefulset replicas = %#v, want 1", statefulSet.Spec.Replicas)
	}
	if statefulSet.Spec.Template.Spec.Containers[0].Image != "apache/nifi:2.10.0" {
		t.Fatalf("image = %q", statefulSet.Spec.Template.Spec.Containers[0].Image)
	}
	command := statefulSet.Spec.Template.Spec.Containers[0].Command
	if len(command) != 3 || !strings.Contains(command[2], "nifi.web.http.port") || !strings.Contains(command[2], "nifi.security.keystore' ''") {
		t.Fatalf("NiFi 2 HTTP startup command = %#v", command)
	}
	if len(statefulSet.Spec.Template.Spec.Volumes) != 1 || statefulSet.Spec.Template.Spec.Volumes[0].EmptyDir == nil {
		t.Fatal("managed cluster without persistence should use an EmptyDir data volume")
	}
	assertEnvironmentValue(t, statefulSet.Spec.Template.Spec.Containers[0].Env, "NIFI_CLUSTER_IS_NODE", "false")

	for _, serviceName := range []string{workloadName, managedClusterHeadlessServiceName(cluster)} {
		service := &corev1.Service{}
		if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: serviceName, Namespace: cluster.Namespace}, service); err != nil {
			t.Fatal(err)
		}
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Ready {
		t.Fatal("cluster ready = true while StatefulSet has no ready replicas")
	}
	if current.Status.Endpoint != "http://production-nifi.default.svc:8080" {
		t.Fatalf("endpoint = %q", current.Status.Endpoint)
	}
	if current.Status.Workload == nil || current.Status.Workload.StatefulSetName != workloadName {
		t.Fatalf("workload status = %#v", current.Status.Workload)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "Provisioning")
}

func TestNiFiClusterReconcileMarksManagedClusterReady(t *testing.T) {
	scheme := managedClusterTestScheme()
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
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ReachabilityChecker: fakeReachabilityChecker{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	statefulSet := &appsv1.StatefulSet{}
	key := types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, statefulSet); err != nil {
		t.Fatal(err)
	}
	statefulSet.Status.ReadyReplicas = 1
	if err := k8sClient.Status().Update(context.Background(), statefulSet); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("managed cluster ready = false, want true")
	}
	if current.Status.Workload == nil || current.Status.Workload.ReadyReplicas != 1 {
		t.Fatalf("workload status = %#v", current.Status.Workload)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionClusterReachable, metav1.ConditionTrue, "ClusterReachable")
}

func TestNiFiClusterReconcileRequiresZooKeeperForMultipleReplicas(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 3,
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).
		Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	current := &nifiv1alpha1.NiFiCluster{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "ConfigurationInvalid")
	statefulSet := &appsv1.StatefulSet{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, statefulSet)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("statefulset lookup error = %v, want not found", err)
	}
}

func TestDesiredManagedClusterStatefulSetConfiguresPersistentCluster(t *testing.T) {
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "dataflows"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 3,
			Coordination: &nifiv1alpha1.NiFiClusterCoordinationSpec{
				ZooKeeperConnectString: "zookeeper.dataflows.svc:2181",
			},
		},
	}

	spec := desiredManagedClusterStatefulSetSpec(cluster)

	if len(spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("volume claim templates = %d, want 1", len(spec.VolumeClaimTemplates))
	}
	storage := spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	if storage.String() != "10Gi" {
		t.Fatalf("storage request = %s, want 10Gi", storage.String())
	}
	if len(spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("init containers = %d, want 1", len(spec.Template.Spec.InitContainers))
	}
	environment := spec.Template.Spec.Containers[0].Env
	assertEnvironmentValue(t, environment, "NIFI_CLUSTER_IS_NODE", "true")
	assertEnvironmentValue(t, environment, "NIFI_ZK_CONNECT_STRING", "zookeeper.dataflows.svc:2181")
	assertEnvironmentValue(t, environment, "NIFI_CLUSTER_ADDRESS", "$(POD_NAME).production-nifi-headless.$(POD_NAMESPACE).svc")
}

func TestNiFiClusterDeleteRefusesUnownedManagedResourceName(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "production",
			Namespace:  "default",
			Finalizers: []string{NiFiControlFinalizer},
		},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:           nifiv1alpha1.ClusterModeInternal,
			DeletionPolicy: nifiv1alpha1.DeletionPolicyDelete,
		},
	}
	unownedService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, unownedService).Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if _, err := reconciler.reconcileClusterDelete(context.Background(), cluster); err == nil {
		t.Fatal("expected deletion to reject an unowned same-named Service")
	}
	currentService := &corev1.Service{}
	if err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(unownedService), currentService); err != nil {
		t.Fatalf("unowned service was removed: %v", err)
	}
}

func managedClusterTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	return scheme
}

func assertEnvironmentValue(t *testing.T, environment []corev1.EnvVar, name string, want string) {
	t.Helper()
	for _, variable := range environment {
		if variable.Name == name {
			if variable.Value != want {
				t.Fatalf("environment %s = %q, want %q", name, variable.Value, want)
			}
			return
		}
	}
	t.Fatalf("environment %s not found", name)
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
