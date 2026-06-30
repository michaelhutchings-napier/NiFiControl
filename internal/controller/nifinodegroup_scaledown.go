package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// nodeGroupScaleDownSpec resolves the effective scale-down policy for a group, falling back
// to the cluster's policy when the group does not set one.
func nodeGroupScaleDownSpec(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) *nifiv1alpha1.NiFiClusterScaleDownSpec {
	if group.Spec.ScaleDown != nil {
		return group.Spec.ScaleDown
	}
	return cluster.Spec.ScaleDown
}

func nodeGroupOffloadEnabled(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) bool {
	spec := nodeGroupScaleDownSpec(cluster, group)
	if spec == nil || spec.OffloadData == nil {
		return true
	}
	return *spec.OffloadData
}

func nodeGroupScaleDownTimeout(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) time.Duration {
	seconds := defaultScaleDownTimeoutSeconds
	if spec := nodeGroupScaleDownSpec(cluster, group); spec != nil && spec.TimeoutSeconds > 0 {
		seconds = spec.TimeoutSeconds
	}
	return time.Duration(seconds) * time.Second
}

func nodeGroupScaleDownForce(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) bool {
	spec := nodeGroupScaleDownSpec(cluster, group)
	return spec != nil && spec.OnTimeout == nifiv1alpha1.ScaleDownTimeoutForce
}

// reconcileNodeGroupScaleDown gracefully offloads the group's nodes before its StatefulSet
// removes their pods, persisting progress on the group status. A group always has the
// cluster's primary pool to receive offloaded data, so even its last node drains gracefully.
func (r *NiFiNodeGroupReconciler) reconcileNodeGroupScaleDown(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup, current *appsv1.StatefulSet) (scaleDownStep, error) {
	desired := nodeGroupReplicas(group)
	if current == nil || current.Spec.Replicas == nil {
		return scaleDownStep{replicas: desired}, r.clearNodeGroupScaleDown(ctx, group)
	}
	currentReplicas := *current.Spec.Replicas
	if currentReplicas <= desired {
		return scaleDownStep{replicas: desired}, r.clearNodeGroupScaleDown(ctx, group)
	}

	step, stepErr := computeOffloadStep(ctx, r.nodeClient(), offloadParams{
		endpoint:        clusterEndpoint(cluster),
		statefulSetName: nodeGroupStatefulSetName(cluster, group),
		headlessService: managedClusterHeadlessServiceName(cluster),
		namespace:       group.Namespace,
		desired:         desired,
		currentReplicas: currentReplicas,
		offloadEnabled:  nodeGroupOffloadEnabled(cluster, group),
		timeout:         nodeGroupScaleDownTimeout(cluster, group),
		force:           nodeGroupScaleDownForce(cluster, group),
	}, group.Status.ScaleDown)

	var statusErr error
	if step.status == nil {
		if group.Status.ScaleDown != nil {
			recordEvent(r.Recorder, group, corev1.EventTypeNormal, "NodeOffloaded",
				fmt.Sprintf("Offloaded and removed node-group node %s; scaling to %d replicas.", group.Status.ScaleDown.NodeAddress, step.replicas))
		}
		statusErr = r.clearNodeGroupScaleDown(ctx, group)
	} else {
		if group.Status.ScaleDown == nil || group.Status.ScaleDown.NodeAddress != step.status.NodeAddress {
			recordEvent(r.Recorder, group, corev1.EventTypeNormal, "OffloadingNode",
				fmt.Sprintf("%s node-group node %s before removal.", step.status.Phase, step.status.NodeAddress))
		}
		statusErr = r.setNodeGroupScaleDown(ctx, group, step.status)
	}
	if stepErr != nil {
		return step, stepErr
	}
	return step, statusErr
}

func (r *NiFiNodeGroupReconciler) setNodeGroupScaleDown(ctx context.Context, group *nifiv1alpha1.NiFiNodeGroup, desired *nifiv1alpha1.NiFiClusterScaleDownStatus) error {
	if scaleDownStatusEqual(group.Status.ScaleDown, desired) {
		return nil
	}
	group.Status.ScaleDown = desired
	return r.statusUpdateNodeGroupScaleDown(ctx, group)
}

func (r *NiFiNodeGroupReconciler) clearNodeGroupScaleDown(ctx context.Context, group *nifiv1alpha1.NiFiNodeGroup) error {
	if group.Status.ScaleDown == nil {
		return nil
	}
	group.Status.ScaleDown = nil
	return r.statusUpdateNodeGroupScaleDown(ctx, group)
}

func (r *NiFiNodeGroupReconciler) statusUpdateNodeGroupScaleDown(ctx context.Context, group *nifiv1alpha1.NiFiNodeGroup) error {
	err := r.Status().Update(ctx, group)
	if apierrors.IsConflict(err) {
		latest := &nifiv1alpha1.NiFiNodeGroup{}
		if getErr := r.Get(ctx, types.NamespacedName{Name: group.Name, Namespace: group.Namespace}, latest); getErr != nil {
			return getErr
		}
		latest.Status.ScaleDown = group.Status.ScaleDown
		return r.Status().Update(ctx, latest)
	}
	return err
}
