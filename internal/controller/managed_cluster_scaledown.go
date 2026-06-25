package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	scaleDownPhaseDisconnecting = "Disconnecting"
	scaleDownPhaseOffloading    = "Offloading"
	scaleDownPhaseRemoving      = "Removing"

	defaultScaleDownTimeoutSeconds = int32(600)
)

// scaleDownDecision is the outcome of one graceful scale-down step.
type scaleDownDecision struct {
	// replicas is the replica count the StatefulSet should currently have. It steps down by
	// one only once the highest-ordinal node has been fully offloaded and removed.
	replicas int32
	// active is true while a graceful scale-down is in progress; the caller requeues and
	// leaves the StatefulSet at the returned replica count instead of finishing reconcile.
	active bool
}

func managedClusterOffloadEnabled(cluster *nifiv1alpha1.NiFiCluster) bool {
	scaleDown := cluster.Spec.ScaleDown
	if scaleDown == nil || scaleDown.OffloadData == nil {
		return true
	}
	return *scaleDown.OffloadData
}

func managedClusterScaleDownTimeout(cluster *nifiv1alpha1.NiFiCluster) time.Duration {
	seconds := defaultScaleDownTimeoutSeconds
	if cluster.Spec.ScaleDown != nil && cluster.Spec.ScaleDown.TimeoutSeconds > 0 {
		seconds = cluster.Spec.ScaleDown.TimeoutSeconds
	}
	return time.Duration(seconds) * time.Second
}

func managedClusterScaleDownTimeoutPolicy(cluster *nifiv1alpha1.NiFiCluster) nifiv1alpha1.ScaleDownTimeoutPolicy {
	if cluster.Spec.ScaleDown != nil && cluster.Spec.ScaleDown.OnTimeout == nifiv1alpha1.ScaleDownTimeoutForce {
		return nifiv1alpha1.ScaleDownTimeoutForce
	}
	return nifiv1alpha1.ScaleDownTimeoutFail
}

// managedClusterNodeAddress returns the NiFi cluster node address for a pod ordinal,
// matching NIFI_CLUSTER_ADDRESS in managedClusterEnvironment
// (<statefulset>-<ordinal>.<headless>.<namespace>.svc).
func managedClusterNodeAddress(cluster *nifiv1alpha1.NiFiCluster, ordinal int32) string {
	return poolNodeAddress(managedClusterResourceName(cluster), managedClusterHeadlessServiceName(cluster), cluster.Namespace, ordinal)
}

// poolNodeAddress returns the NiFi cluster node address for a pod ordinal of any pool's
// StatefulSet, matching NIFI_CLUSTER_ADDRESS.
func poolNodeAddress(statefulSetName, headlessService, namespace string, ordinal int32) string {
	return fmt.Sprintf("%s-%d.%s.%s.svc", statefulSetName, ordinal, headlessService, namespace)
}

func (r *NiFiClusterReconciler) clusterNodeClient() nifi.ClusterNodeClient {
	if r.ClusterNodeClient != nil {
		return r.ClusterNodeClient
	}
	return nifi.HTTPClusterNodeClient{}
}

// offloadParams describes a single pool's graceful scale-down: its StatefulSet name and the
// shared cluster context needed to drain and remove its highest-ordinal node.
type offloadParams struct {
	endpoint        string
	statefulSetName string
	headlessService string
	namespace       string
	desired         int32
	currentReplicas int32
	offloadEnabled  bool
	timeout         time.Duration
	force           bool
}

// scaleDownStep is the outcome of one graceful offload step for a pool.
type scaleDownStep struct {
	replicas int32                                    // replica count the StatefulSet should have now
	active   bool                                     // true while a scale-down is in progress
	status   *nifiv1alpha1.NiFiClusterScaleDownStatus // desired ScaleDown status (nil clears it)
}

// computeOffloadStep advances graceful node offload for one pool by a single step. It is
// shared by the cluster's primary pool and NiFiNodeGroup pools; callers persist the returned
// status on their own object. Each call drives the highest-ordinal node one step through
// disconnect -> offload -> delete, decrementing the replica count by one only once the node
// has left the NiFi cluster.
func computeOffloadStep(ctx context.Context, nodeClient nifi.ClusterNodeClient, p offloadParams, current *nifiv1alpha1.NiFiClusterScaleDownStatus) (scaleDownStep, error) {
	// Scale-up, steady state, or offload disabled. Callers are responsible for not enabling
	// offload when there is no cluster left to receive the data (the primary pool guards
	// currentReplicas <= 1; a node group always has the primary pool to offload to).
	if p.currentReplicas <= p.desired || !p.offloadEnabled {
		return scaleDownStep{replicas: p.desired}, nil
	}
	address := poolNodeAddress(p.statefulSetName, p.headlessService, p.namespace, p.currentReplicas-1)
	nodes, err := nodeClient.ListClusterNodes(ctx, p.endpoint)
	if err != nil {
		return scaleDownStep{replicas: p.currentReplicas, active: true, status: current}, fmt.Errorf("list NiFi cluster nodes for scale-down: %w", err)
	}
	node := findClusterNodeByAddress(nodes, address)
	if node == nil {
		// Already gone from the NiFi cluster; allow the StatefulSet to drop its pod.
		return scaleDownStep{replicas: p.currentReplicas - 1, active: true}, nil
	}
	startedAt := offloadStartedAt(current, address)
	statusFor := func(phase string) *nifiv1alpha1.NiFiClusterScaleDownStatus {
		started := startedAt
		return &nifiv1alpha1.NiFiClusterScaleDownStatus{NodeAddress: address, Phase: phase, StartedAt: &started}
	}
	if time.Since(startedAt.Time) > p.timeout {
		if p.force {
			if err := nodeClient.DeleteClusterNode(ctx, p.endpoint, node.NodeID); err != nil && !nifi.IsNotFound(err) {
				return scaleDownStep{replicas: p.currentReplicas, active: true, status: statusFor(scaleDownPhaseRemoving)}, fmt.Errorf("force-remove NiFi node %s after offload timeout: %w", address, err)
			}
			return scaleDownStep{replicas: p.currentReplicas - 1, active: true}, nil
		}
		return scaleDownStep{replicas: p.currentReplicas, active: true, status: statusFor(node.Status)}, fmt.Errorf("NiFi node %s did not finish offloading within %s", address, p.timeout)
	}

	switch node.Status {
	case nifi.NodeStatusConnected, nifi.NodeStatusConnecting:
		if err := nodeClient.SetClusterNodeState(ctx, p.endpoint, node.NodeID, nifi.NodeStatusDisconnecting); err != nil {
			return scaleDownStep{replicas: p.currentReplicas, active: true, status: current}, fmt.Errorf("disconnect NiFi node %s: %w", address, err)
		}
		return scaleDownStep{replicas: p.currentReplicas, active: true, status: statusFor(scaleDownPhaseDisconnecting)}, nil
	case nifi.NodeStatusDisconnected:
		if err := nodeClient.SetClusterNodeState(ctx, p.endpoint, node.NodeID, nifi.NodeStatusOffloading); err != nil {
			return scaleDownStep{replicas: p.currentReplicas, active: true, status: current}, fmt.Errorf("offload NiFi node %s: %w", address, err)
		}
		return scaleDownStep{replicas: p.currentReplicas, active: true, status: statusFor(scaleDownPhaseOffloading)}, nil
	case nifi.NodeStatusOffloaded:
		if err := nodeClient.DeleteClusterNode(ctx, p.endpoint, node.NodeID); err != nil && !nifi.IsNotFound(err) {
			return scaleDownStep{replicas: p.currentReplicas, active: true, status: current}, fmt.Errorf("remove offloaded NiFi node %s: %w", address, err)
		}
		return scaleDownStep{replicas: p.currentReplicas - 1, active: true, status: statusFor(scaleDownPhaseRemoving)}, nil
	default:
		phase := scaleDownPhaseOffloading
		if node.Status == nifi.NodeStatusDisconnecting {
			phase = scaleDownPhaseDisconnecting
		}
		return scaleDownStep{replicas: p.currentReplicas, active: true, status: statusFor(phase)}, nil
	}
}

// reconcileManagedClusterScaleDown gracefully offloads the cluster's primary-pool nodes
// before the StatefulSet removes their pods, persisting progress on the cluster status.
func (r *NiFiClusterReconciler) reconcileManagedClusterScaleDown(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, current *appsv1.StatefulSet) (scaleDownDecision, error) {
	desired := managedClusterReplicas(cluster)
	if current == nil || current.Spec.Replicas == nil {
		return scaleDownDecision{replicas: desired}, r.clearScaleDownStatus(ctx, cluster)
	}
	currentReplicas := *current.Spec.Replicas
	if currentReplicas <= desired || !managedClusterOffloadEnabled(cluster) || currentReplicas <= 1 {
		return scaleDownDecision{replicas: desired}, r.clearScaleDownStatus(ctx, cluster)
	}

	// Offload requires an authenticated client for the running cluster's REST API. Configure
	// it just-in-time so a client-config error never blocks the normal create/update path.
	if err := configureClusterHTTPClient(ctx, r.Client, cluster); err != nil {
		return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("configure NiFi API client for scale-down: %w", err)
	}
	step, stepErr := computeOffloadStep(ctx, r.clusterNodeClient(), offloadParams{
		endpoint:        managedClusterEndpoint(cluster),
		statefulSetName: managedClusterResourceName(cluster),
		headlessService: managedClusterHeadlessServiceName(cluster),
		namespace:       cluster.Namespace,
		desired:         desired,
		currentReplicas: currentReplicas,
		offloadEnabled:  true,
		timeout:         managedClusterScaleDownTimeout(cluster),
		force:           managedClusterScaleDownTimeoutPolicy(cluster) == nifiv1alpha1.ScaleDownTimeoutForce,
	}, cluster.Status.ScaleDown)

	var statusErr error
	if step.status == nil {
		statusErr = r.clearScaleDownStatus(ctx, cluster)
	} else {
		statusErr = r.setScaleDownStatus(ctx, cluster, step.status)
	}
	if stepErr != nil {
		return scaleDownDecision{replicas: step.replicas, active: step.active}, stepErr
	}
	return scaleDownDecision{replicas: step.replicas, active: step.active}, statusErr
}

func findClusterNodeByAddress(nodes []nifi.ClusterNode, address string) *nifi.ClusterNode {
	for i := range nodes {
		if nodes[i].Address == address {
			return &nodes[i]
		}
	}
	return nil
}

// offloadStartedAt returns the offload start time for the given node address, beginning a
// fresh timer when the address changes (or there is no prior status).
func offloadStartedAt(current *nifiv1alpha1.NiFiClusterScaleDownStatus, address string) metav1.Time {
	if current != nil && current.NodeAddress == address && current.StartedAt != nil {
		return *current.StartedAt
	}
	return metav1.Now()
}

func (r *NiFiClusterReconciler) setScaleDownStatus(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, desired *nifiv1alpha1.NiFiClusterScaleDownStatus) error {
	if scaleDownStatusEqual(cluster.Status.ScaleDown, desired) {
		return nil
	}
	cluster.Status.ScaleDown = desired
	return r.statusUpdateScaleDown(ctx, cluster)
}

func (r *NiFiClusterReconciler) clearScaleDownStatus(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	if cluster.Status.ScaleDown == nil {
		return nil
	}
	cluster.Status.ScaleDown = nil
	return r.statusUpdateScaleDown(ctx, cluster)
}

// statusUpdateScaleDown persists the ScaleDown status, refetching on conflict so the
// frequent in-progress updates do not abort reconcile on a stale resource version.
func (r *NiFiClusterReconciler) statusUpdateScaleDown(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	err := r.Status().Update(ctx, cluster)
	if apierrors.IsConflict(err) {
		latest := &nifiv1alpha1.NiFiCluster{}
		if getErr := r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, latest); getErr != nil {
			return getErr
		}
		latest.Status.ScaleDown = cluster.Status.ScaleDown
		return r.Status().Update(ctx, latest)
	}
	return err
}

func scaleDownStatusEqual(left, right *nifiv1alpha1.NiFiClusterScaleDownStatus) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.NodeAddress == right.NodeAddress && left.Phase == right.Phase
}
