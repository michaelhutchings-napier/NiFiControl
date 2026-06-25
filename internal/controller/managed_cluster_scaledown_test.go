package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fakeClusterNodeClient records cluster-node calls and simulates NiFi finishing a requested
// state transition by the next reconcile (DISCONNECTING -> DISCONNECTED, OFFLOADING ->
// OFFLOADED).
type fakeClusterNodeClient struct {
	nodes    []nifi.ClusterNode
	listErr  error
	setCalls []string
	deleted  []string
}

func (f *fakeClusterNodeClient) ListClusterNodes(_ context.Context, _ string) ([]nifi.ClusterNode, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.nodes, nil
}

func (f *fakeClusterNodeClient) SetClusterNodeState(_ context.Context, _ string, nodeID string, state string) error {
	f.setCalls = append(f.setCalls, nodeID+":"+state)
	for i := range f.nodes {
		if f.nodes[i].NodeID != nodeID {
			continue
		}
		switch state {
		case nifi.NodeStatusDisconnecting:
			f.nodes[i].Status = nifi.NodeStatusDisconnected
		case nifi.NodeStatusOffloading:
			f.nodes[i].Status = nifi.NodeStatusOffloaded
		}
	}
	return nil
}

func (f *fakeClusterNodeClient) DeleteClusterNode(_ context.Context, _ string, nodeID string) error {
	f.deleted = append(f.deleted, nodeID)
	return nil
}

func scaleDownCluster(desired int32) *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "dataflows", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:         nifiv1alpha1.ClusterModeInternal,
			Replicas:     desired,
			Coordination: &nifiv1alpha1.NiFiClusterCoordinationSpec{ZooKeeperConnectString: "zk:2181"},
		},
	}
}

func statefulSetWithReplicas(cluster *nifiv1alpha1.NiFiCluster, replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(replicas)},
	}
}

func TestManagedClusterNodeAddressMatchesEnvironment(t *testing.T) {
	cluster := scaleDownCluster(3)
	got := managedClusterNodeAddress(cluster, 2)
	want := "production-nifi-2.production-nifi-headless.dataflows.svc"
	if got != want {
		t.Fatalf("node address = %q, want %q", got, want)
	}
}

func TestScaleDownGracefullyOffloadsTopNode(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(2) // scaling 3 -> 2
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	topAddress := managedClusterNodeAddress(cluster, 2)
	nodeClient := &fakeClusterNodeClient{nodes: []nifi.ClusterNode{
		{NodeID: "n0", Address: managedClusterNodeAddress(cluster, 0), Status: nifi.NodeStatusConnected},
		{NodeID: "n1", Address: managedClusterNodeAddress(cluster, 1), Status: nifi.NodeStatusConnected},
		{NodeID: "n2", Address: topAddress, Status: nifi.NodeStatusConnected},
	}}
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	ctx := context.Background()
	current := statefulSetWithReplicas(cluster, 3)

	// Step 1: connected -> request DISCONNECTING, hold replicas at 3.
	decision, err := r.reconcileManagedClusterScaleDown(ctx, cluster, current)
	if err != nil {
		t.Fatal(err)
	}
	if decision.replicas != 3 || !decision.active {
		t.Fatalf("step1 decision = %#v, want {3 true}", decision)
	}
	if cluster.Status.ScaleDown == nil || cluster.Status.ScaleDown.Phase != scaleDownPhaseDisconnecting || cluster.Status.ScaleDown.NodeAddress != topAddress {
		t.Fatalf("step1 status = %#v", cluster.Status.ScaleDown)
	}

	// Step 2: disconnected -> request OFFLOADING, still hold at 3.
	decision, err = r.reconcileManagedClusterScaleDown(ctx, cluster, current)
	if err != nil {
		t.Fatal(err)
	}
	if decision.replicas != 3 || !decision.active {
		t.Fatalf("step2 decision = %#v, want {3 true}", decision)
	}
	if cluster.Status.ScaleDown.Phase != scaleDownPhaseOffloading {
		t.Fatalf("step2 phase = %q", cluster.Status.ScaleDown.Phase)
	}

	// Step 3: offloaded -> delete node, allow shrink to 2.
	decision, err = r.reconcileManagedClusterScaleDown(ctx, cluster, current)
	if err != nil {
		t.Fatal(err)
	}
	if decision.replicas != 2 || !decision.active {
		t.Fatalf("step3 decision = %#v, want {2 true}", decision)
	}
	if len(nodeClient.deleted) != 1 || nodeClient.deleted[0] != "n2" {
		t.Fatalf("deleted = %#v, want [n2]", nodeClient.deleted)
	}
	wantSet := []string{"n2:" + nifi.NodeStatusDisconnecting, "n2:" + nifi.NodeStatusOffloading}
	if len(nodeClient.setCalls) != 2 || nodeClient.setCalls[0] != wantSet[0] || nodeClient.setCalls[1] != wantSet[1] {
		t.Fatalf("setCalls = %#v, want %#v", nodeClient.setCalls, wantSet)
	}

	// Step 4: StatefulSet has shrunk and the node has left the cluster; scale-down is done.
	nodeClient.nodes = nodeClient.nodes[:2]
	decision, err = r.reconcileManagedClusterScaleDown(ctx, cluster, statefulSetWithReplicas(cluster, 2))
	if err != nil {
		t.Fatal(err)
	}
	if decision.replicas != 2 || decision.active {
		t.Fatalf("step4 decision = %#v, want {2 false}", decision)
	}
	if cluster.Status.ScaleDown != nil {
		t.Fatalf("step4 status should be cleared, got %#v", cluster.Status.ScaleDown)
	}
}

func TestScaleDownNodeAlreadyGoneAllowsShrink(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(2)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	nodeClient := &fakeClusterNodeClient{nodes: []nifi.ClusterNode{
		{NodeID: "n0", Address: managedClusterNodeAddress(cluster, 0), Status: nifi.NodeStatusConnected},
		{NodeID: "n1", Address: managedClusterNodeAddress(cluster, 1), Status: nifi.NodeStatusConnected},
	}}
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	decision, err := r.reconcileManagedClusterScaleDown(context.Background(), cluster, statefulSetWithReplicas(cluster, 3))
	if err != nil {
		t.Fatal(err)
	}
	if decision.replicas != 2 || !decision.active {
		t.Fatalf("decision = %#v, want {2 true}", decision)
	}
	if len(nodeClient.setCalls) != 0 || len(nodeClient.deleted) != 0 {
		t.Fatalf("no cluster calls expected, got set=%#v deleted=%#v", nodeClient.setCalls, nodeClient.deleted)
	}
}

func TestScaleDownDisabledShrinksImmediately(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(2)
	cluster.Spec.ScaleDown = &nifiv1alpha1.NiFiClusterScaleDownSpec{OffloadData: ptr.To(false)}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	nodeClient := &fakeClusterNodeClient{}
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	decision, err := r.reconcileManagedClusterScaleDown(context.Background(), cluster, statefulSetWithReplicas(cluster, 3))
	if err != nil {
		t.Fatal(err)
	}
	if decision.replicas != 2 || decision.active {
		t.Fatalf("decision = %#v, want {2 false}", decision)
	}
	if len(nodeClient.setCalls) != 0 {
		t.Fatalf("offload disabled should make no cluster calls, got %#v", nodeClient.setCalls)
	}
}

func TestScaleDownSingleNodeShrinksImmediately(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(0) // tearing down the last node
	cluster.Spec.Coordination = nil
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	nodeClient := &fakeClusterNodeClient{}
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	decision, err := r.reconcileManagedClusterScaleDown(context.Background(), cluster, statefulSetWithReplicas(cluster, 1))
	if err != nil {
		t.Fatal(err)
	}
	// desired clamps to 1 via managedClusterReplicas, currentReplicas(1) <= desired(1): no-op.
	if decision.active {
		t.Fatalf("single-node scale should not offload, decision = %#v", decision)
	}
}

func TestScaleDownTimeoutForceRemovesNode(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(2)
	cluster.Spec.ScaleDown = &nifiv1alpha1.NiFiClusterScaleDownSpec{TimeoutSeconds: 1, OnTimeout: nifiv1alpha1.ScaleDownTimeoutForce}
	topAddress := managedClusterNodeAddress(cluster, 2)
	cluster.Status.ScaleDown = &nifiv1alpha1.NiFiClusterScaleDownStatus{
		NodeAddress: topAddress,
		Phase:       scaleDownPhaseOffloading,
		StartedAt:   &metav1.Time{Time: time.Now().Add(-10 * time.Minute)},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	nodeClient := &fakeClusterNodeClient{nodes: []nifi.ClusterNode{
		{NodeID: "n0", Address: managedClusterNodeAddress(cluster, 0), Status: nifi.NodeStatusConnected},
		{NodeID: "n1", Address: managedClusterNodeAddress(cluster, 1), Status: nifi.NodeStatusConnected},
		{NodeID: "n2", Address: topAddress, Status: nifi.NodeStatusOffloading}, // stuck
	}}
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	decision, err := r.reconcileManagedClusterScaleDown(context.Background(), cluster, statefulSetWithReplicas(cluster, 3))
	if err != nil {
		t.Fatal(err)
	}
	if decision.replicas != 2 || !decision.active {
		t.Fatalf("decision = %#v, want {2 true}", decision)
	}
	if len(nodeClient.deleted) != 1 || nodeClient.deleted[0] != "n2" {
		t.Fatalf("force timeout should delete n2, deleted = %#v", nodeClient.deleted)
	}
}

func TestScaleDownTimeoutFailReturnsError(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(2)
	cluster.Spec.ScaleDown = &nifiv1alpha1.NiFiClusterScaleDownSpec{TimeoutSeconds: 1, OnTimeout: nifiv1alpha1.ScaleDownTimeoutFail}
	topAddress := managedClusterNodeAddress(cluster, 2)
	cluster.Status.ScaleDown = &nifiv1alpha1.NiFiClusterScaleDownStatus{
		NodeAddress: topAddress,
		Phase:       scaleDownPhaseOffloading,
		StartedAt:   &metav1.Time{Time: time.Now().Add(-10 * time.Minute)},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	nodeClient := &fakeClusterNodeClient{nodes: []nifi.ClusterNode{
		{NodeID: "n2", Address: topAddress, Status: nifi.NodeStatusOffloading},
	}}
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	_, err := r.reconcileManagedClusterScaleDown(context.Background(), cluster, statefulSetWithReplicas(cluster, 3))
	if err == nil {
		t.Fatal("expected an error when offload exceeds timeout with Fail policy")
	}
	if len(nodeClient.deleted) != 0 {
		t.Fatalf("Fail policy must not delete the node, deleted = %#v", nodeClient.deleted)
	}
}

func TestScaleDownListErrorHoldsReplicas(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := scaleDownCluster(2)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	nodeClient := &fakeClusterNodeClient{listErr: errors.New("unreachable")}
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ClusterNodeClient: nodeClient}
	decision, err := r.reconcileManagedClusterScaleDown(context.Background(), cluster, statefulSetWithReplicas(cluster, 3))
	if err == nil {
		t.Fatal("expected error when listing nodes fails")
	}
	if decision.replicas != 3 {
		t.Fatalf("on list error the StatefulSet must hold at 3, got %d", decision.replicas)
	}
}
