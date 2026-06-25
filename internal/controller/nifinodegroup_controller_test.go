package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func readyClusteredCluster() *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:         nifiv1alpha1.ClusterModeInternal,
			Replicas:     3,
			Coordination: &nifiv1alpha1.NiFiClusterCoordinationSpec{ZooKeeperConnectString: "zk.default.svc:2181"},
		},
		Status: nifiv1alpha1.NiFiClusterStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1},
			Endpoint:     "http://production-nifi.default.svc:8080",
		},
	}
}

func TestDesiredNodeGroupStatefulSetSpec(t *testing.T) {
	cluster := readyClusteredCluster()
	storageEnabled := true
	group := &nifiv1alpha1.NiFiNodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "default"},
		Spec: nifiv1alpha1.NiFiNodeGroupSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
			Replicas:   2,
			JVM:        &nifiv1alpha1.NiFiClusterJVMSpec{HeapInitial: "4g", HeapMax: "6g"},
			Storage:    &nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storageEnabled, Size: resource.MustParse("100Gi")},
			Scheduling: &nifiv1alpha1.NiFiClusterScheduling{NodeSelector: map[string]string{"workload": "nifi-workers"}},
		},
	}
	spec := desiredNodeGroupStatefulSetSpec(cluster, group, nil, 2, "")

	if spec.ServiceName != managedClusterHeadlessServiceName(cluster) {
		t.Fatalf("serviceName = %q, want the cluster headless service", spec.ServiceName)
	}
	if spec.Template.Labels[nodePoolLabel] != "workers" {
		t.Fatalf("pod node-pool label = %q, want workers", spec.Template.Labels[nodePoolLabel])
	}
	if spec.Template.Labels["app.kubernetes.io/component"] != "nifi-node" {
		t.Fatal("group pods must carry the cluster node label so the headless Service selects them")
	}
	env := spec.Template.Spec.Containers[0].Env
	assertEnvironmentValue(t, env, "NIFI_JVM_HEAP_MAX", "6g")
	assertEnvironmentValue(t, env, "NIFI_CLUSTER_IS_NODE", "true")
	assertEnvironmentValue(t, env, "NIFI_ZK_CONNECT_STRING", "zk.default.svc:2181")
	var hasSensitive bool
	for _, e := range env {
		if e.Name == "NIFI_SENSITIVE_PROPS_KEY" && e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			hasSensitive = e.ValueFrom.SecretKeyRef.Name == managedClusterSensitivePropsSecretName(cluster)
		}
	}
	if !hasSensitive {
		t.Fatal("group pods must share the cluster sensitive-props key Secret")
	}
	if len(spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected a volume claim template for group storage, got %d", len(spec.VolumeClaimTemplates))
	}
	if spec.Template.Spec.NodeSelector["workload"] != "nifi-workers" {
		t.Fatalf("group scheduling not applied: %#v", spec.Template.Spec.NodeSelector)
	}
}

func TestNiFiNodeGroupReconcileCreatesStatefulSet(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := readyClusteredCluster()
	group := &nifiv1alpha1.NiFiNodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiNodeGroupSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"}, Replicas: 2},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, group).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiNodeGroup{}).Build()
	r := &NiFiNodeGroupReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: &fakeClusterNodeClient{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: group.Name, Namespace: group.Namespace}}
	// First reconcile adds the finalizer; second creates the StatefulSet and sets status.
	for range 2 {
		if _, err := r.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	sts := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "production-nifi-workers", Namespace: "default"}, sts); err != nil {
		t.Fatalf("group StatefulSet not created: %v", err)
	}
	if sts.Spec.ServiceName != "production-nifi-headless" {
		t.Fatalf("serviceName = %q", sts.Spec.ServiceName)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 2 {
		t.Fatalf("replicas = %v", sts.Spec.Replicas)
	}
	if sts.Spec.Template.Labels[nodePoolLabel] != "workers" {
		t.Fatalf("pod node-pool label = %q", sts.Spec.Template.Labels[nodePoolLabel])
	}
	if len(sts.OwnerReferences) != 1 || sts.OwnerReferences[0].Kind != "NiFiNodeGroup" {
		t.Fatalf("group StatefulSet should be owned by the NiFiNodeGroup, got %#v", sts.OwnerReferences)
	}

	current := &nifiv1alpha1.NiFiNodeGroup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Selector != nodeGroupScaleSelector(cluster, group) {
		t.Fatalf("scale selector = %q", current.Status.Selector)
	}
	if current.Status.StatefulSetName != "production-nifi-workers" {
		t.Fatalf("statefulSetName = %q", current.Status.StatefulSetName)
	}
}

func TestNiFiNodeGroupRejectsNonClusteredCluster(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := readyClusteredCluster()
	cluster.Spec.Replicas = 1
	cluster.Spec.Coordination = nil
	group := &nifiv1alpha1.NiFiNodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "default", Generation: 1, Finalizers: []string{NiFiControlFinalizer}},
		Spec:       nifiv1alpha1.NiFiNodeGroupSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"}, Replicas: 2},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, group).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiNodeGroup{}).Build()
	r := &NiFiNodeGroupReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: &fakeClusterNodeClient{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: group.Name, Namespace: group.Namespace}}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	current := &nifiv1alpha1.NiFiNodeGroup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Ready {
		t.Fatal("a node group on a non-clustered cluster must not be Ready")
	}
	sts := &appsv1.StatefulSet{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "production-nifi-workers", Namespace: "default"}, sts); err == nil {
		t.Fatal("no StatefulSet should be created for a non-clustered cluster")
	}
}

func TestNiFiNodeGroupScaleDownOffloads(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := readyClusteredCluster()
	group := &nifiv1alpha1.NiFiNodeGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiNodeGroupSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"}, Replicas: 2},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, group).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiNodeGroup{}).Build()
	topAddress := poolNodeAddress("production-nifi-workers", "production-nifi-headless", "default", 2)
	nodeClient := &fakeClusterNodeClient{nodes: []nifi.ClusterNode{
		{NodeID: "w2", Address: topAddress, Status: nifi.NodeStatusConnected},
	}}
	r := &NiFiNodeGroupReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	current := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "production-nifi-workers", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(3))},
	}
	step, err := r.reconcileNodeGroupScaleDown(context.Background(), cluster, group, current)
	if err != nil {
		t.Fatal(err)
	}
	if step.replicas != 3 || !step.active {
		t.Fatalf("step = %#v, want {3 true}", step)
	}
	if len(nodeClient.setCalls) != 1 || nodeClient.setCalls[0] != "w2:"+nifi.NodeStatusDisconnecting {
		t.Fatalf("expected the top group node to be disconnected, got %#v", nodeClient.setCalls)
	}
	if group.Status.ScaleDown == nil || group.Status.ScaleDown.Phase != scaleDownPhaseDisconnecting {
		t.Fatalf("group scaleDown status = %#v", group.Status.ScaleDown)
	}
}
