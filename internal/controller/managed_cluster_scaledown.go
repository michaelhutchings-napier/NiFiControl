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
	return fmt.Sprintf("%s-%d.%s.%s.svc", managedClusterResourceName(cluster), ordinal, managedClusterHeadlessServiceName(cluster), cluster.Namespace)
}

func (r *NiFiClusterReconciler) clusterNodeClient() nifi.ClusterNodeClient {
	if r.ClusterNodeClient != nil {
		return r.ClusterNodeClient
	}
	return nifi.HTTPClusterNodeClient{}
}

// reconcileManagedClusterScaleDown gracefully offloads NiFi nodes before the StatefulSet
// removes their pods. It returns the replica count the StatefulSet should currently have
// and whether a scale-down is still in progress. Each call advances the highest-ordinal
// node one step through disconnect -> offload -> delete, decrementing the desired replica
// count by one only after that node has left the NiFi cluster.
func (r *NiFiClusterReconciler) reconcileManagedClusterScaleDown(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, current *appsv1.StatefulSet) (scaleDownDecision, error) {
	desired := managedClusterReplicas(cluster)
	if current == nil || current.Spec.Replicas == nil {
		return scaleDownDecision{replicas: desired}, r.clearScaleDownStatus(ctx, cluster)
	}
	currentReplicas := *current.Spec.Replicas
	// Scale-up or steady state: nothing to offload.
	if currentReplicas <= desired {
		return scaleDownDecision{replicas: desired}, r.clearScaleDownStatus(ctx, cluster)
	}
	// Immediate shrink when offload is disabled, or there is no NiFi cluster to receive the
	// offloaded data (a single node, or a teardown to zero).
	if !managedClusterOffloadEnabled(cluster) || currentReplicas <= 1 {
		return scaleDownDecision{replicas: desired}, r.clearScaleDownStatus(ctx, cluster)
	}

	endpoint := managedClusterEndpoint(cluster)
	ordinal := currentReplicas - 1
	address := managedClusterNodeAddress(cluster, ordinal)

	// Offload requires an authenticated client for the running cluster's REST API. Configure
	// it just-in-time so a client-config error never blocks the normal create/update path.
	if err := configureClusterHTTPClient(ctx, r.Client, cluster); err != nil {
		return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("configure NiFi API client for scale-down: %w", err)
	}

	nodes, err := r.clusterNodeClient().ListClusterNodes(ctx, endpoint)
	if err != nil {
		return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("list NiFi cluster nodes for scale-down: %w", err)
	}
	node := findClusterNodeByAddress(nodes, address)
	if node == nil {
		// The node has already left the NiFi cluster; allow the StatefulSet to drop its pod.
		return scaleDownDecision{replicas: currentReplicas - 1, active: true}, r.clearScaleDownStatus(ctx, cluster)
	}

	startedAt := r.scaleDownStartedAt(cluster, address)
	if time.Since(startedAt.Time) > managedClusterScaleDownTimeout(cluster) {
		if managedClusterScaleDownTimeoutPolicy(cluster) == nifiv1alpha1.ScaleDownTimeoutForce {
			if err := r.clusterNodeClient().DeleteClusterNode(ctx, endpoint, node.NodeID); err != nil && !nifi.IsNotFound(err) {
				return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("force-remove NiFi node %s after offload timeout: %w", address, err)
			}
			return scaleDownDecision{replicas: currentReplicas - 1, active: true}, r.clearScaleDownStatus(ctx, cluster)
		}
		return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("NiFi node %s did not finish offloading within %s", address, managedClusterScaleDownTimeout(cluster))
	}

	switch node.Status {
	case nifi.NodeStatusConnected, nifi.NodeStatusConnecting:
		if err := r.clusterNodeClient().SetClusterNodeState(ctx, endpoint, node.NodeID, nifi.NodeStatusDisconnecting); err != nil {
			return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("disconnect NiFi node %s: %w", address, err)
		}
		return scaleDownDecision{replicas: currentReplicas, active: true}, r.setScaleDownStatus(ctx, cluster, address, scaleDownPhaseDisconnecting, startedAt)
	case nifi.NodeStatusDisconnected:
		if err := r.clusterNodeClient().SetClusterNodeState(ctx, endpoint, node.NodeID, nifi.NodeStatusOffloading); err != nil {
			return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("offload NiFi node %s: %w", address, err)
		}
		return scaleDownDecision{replicas: currentReplicas, active: true}, r.setScaleDownStatus(ctx, cluster, address, scaleDownPhaseOffloading, startedAt)
	case nifi.NodeStatusOffloaded:
		if err := r.clusterNodeClient().DeleteClusterNode(ctx, endpoint, node.NodeID); err != nil && !nifi.IsNotFound(err) {
			return scaleDownDecision{replicas: currentReplicas, active: true}, fmt.Errorf("remove offloaded NiFi node %s: %w", address, err)
		}
		// The node is gone from the cluster; let the StatefulSet drop its pod.
		return scaleDownDecision{replicas: currentReplicas - 1, active: true}, r.setScaleDownStatus(ctx, cluster, address, scaleDownPhaseRemoving, startedAt)
	default:
		// DISCONNECTING / OFFLOADING and any transient state: keep waiting.
		phase := scaleDownPhaseOffloading
		if node.Status == nifi.NodeStatusDisconnecting {
			phase = scaleDownPhaseDisconnecting
		}
		return scaleDownDecision{replicas: currentReplicas, active: true}, r.setScaleDownStatus(ctx, cluster, address, phase, startedAt)
	}
}

func findClusterNodeByAddress(nodes []nifi.ClusterNode, address string) *nifi.ClusterNode {
	for i := range nodes {
		if nodes[i].Address == address {
			return &nodes[i]
		}
	}
	return nil
}

// scaleDownStartedAt returns the offload start time for the given node address, beginning a
// fresh timer when the address changes.
func (r *NiFiClusterReconciler) scaleDownStartedAt(cluster *nifiv1alpha1.NiFiCluster, address string) metav1.Time {
	if cluster.Status.ScaleDown != nil && cluster.Status.ScaleDown.NodeAddress == address && cluster.Status.ScaleDown.StartedAt != nil {
		return *cluster.Status.ScaleDown.StartedAt
	}
	return metav1.Now()
}

func (r *NiFiClusterReconciler) setScaleDownStatus(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, address, phase string, startedAt metav1.Time) error {
	desired := &nifiv1alpha1.NiFiClusterScaleDownStatus{NodeAddress: address, Phase: phase, StartedAt: &startedAt}
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
