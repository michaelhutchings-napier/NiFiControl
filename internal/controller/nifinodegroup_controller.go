package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifinodegroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifinodegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifinodegroups/finalizers,verbs=update

// NiFiNodeGroupReconciler manages an additional, independently-scalable pool of NiFi nodes
// that join an existing operator-managed cluster. It creates a StatefulSet that shares the
// cluster's headless Service, ZooKeeper, sensitive-properties key, and TLS materials, and
// gracefully offloads the group's nodes on scale-down.
type NiFiNodeGroupReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	ClusterNodeClient nifi.ClusterNodeClient
	// Recorder emits Kubernetes Events for notable lifecycle transitions (optional).
	Recorder record.EventRecorder
}

func (r *NiFiNodeGroupReconciler) nodeClient() nifi.ClusterNodeClient {
	if r.ClusterNodeClient != nil {
		return r.ClusterNodeClient
	}
	return nifi.HTTPClusterNodeClient{}
}

func (r *NiFiNodeGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	group := &nifiv1alpha1.NiFiNodeGroup{}
	if err := r.Get(ctx, req.NamespacedName, group); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !group.DeletionTimestamp.IsZero() {
		return r.reconcileNodeGroupDelete(ctx, group)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, group); err != nil || updated {
		return ctrl.Result{}, err
	}

	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, group.Namespace, group.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markNodeGroupWaiting(ctx, group, waitingFor)
	}
	if resolvedClusterMode(cluster) != nifiv1alpha1.ClusterModeInternal {
		return ctrl.Result{}, r.setNodeGroupStatus(ctx, group, cluster, false, "ClusterNotManaged", "Node groups require an operator-managed (Internal) NiFiCluster.", 0)
	}
	if cluster.Namespace != group.Namespace {
		return ctrl.Result{}, r.setNodeGroupStatus(ctx, group, cluster, false, "ClusterNamespaceMismatch", "A NiFiNodeGroup must be in the same namespace as its cluster.", 0)
	}
	if cluster.Spec.Coordination == nil {
		return ctrl.Result{}, r.setNodeGroupStatus(ctx, group, cluster, false, "ClusterNotClustered", "Node groups require a clustered cluster (spec.coordination must be set).", 0)
	}

	tls, tlsReady, err := r.resolveClusterTLS(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !tlsReady {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.setNodeGroupStatus(ctx, group, cluster, false, "TLSPending", "Waiting for the cluster's internal TLS materials to be ready.", 0)
	}

	desired := nodeGroupReplicas(group)
	stsName := nodeGroupStatefulSetName(cluster, group)
	group.Status.StatefulSetName = stsName
	existing := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: group.Namespace}, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		existing = nil
	}
	currentReady := int32(0)
	if existing != nil {
		currentReady = existing.Status.ReadyReplicas
	}

	step, err := r.reconcileNodeGroupScaleDown(ctx, cluster, group, existing)
	if err != nil {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.setNodeGroupStatus(ctx, group, cluster, false, "ScaleDownFailed", err.Error(), currentReady)
	}

	statefulSet, err := r.reconcileNodeGroupStatefulSet(ctx, cluster, group, tls, step.replicas)
	if err != nil {
		return ctrl.Result{}, r.setNodeGroupStatus(ctx, group, cluster, false, "StatefulSetReconcileFailed", err.Error(), currentReady)
	}
	if step.active {
		message := fmt.Sprintf("Gracefully offloading nodes during scale-down to %d replicas.", desired)
		if group.Status.ScaleDown != nil && group.Status.ScaleDown.NodeAddress != "" {
			message = fmt.Sprintf("Scaling down: %s node %s.", group.Status.ScaleDown.Phase, group.Status.ScaleDown.NodeAddress)
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, r.setNodeGroupStatus(ctx, group, cluster, false, "ScalingDown", message, statefulSet.Status.ReadyReplicas)
	}

	upgrading := statefulSet.Status.UpdateRevision != "" && statefulSet.Status.CurrentRevision != "" &&
		statefulSet.Status.UpdateRevision != statefulSet.Status.CurrentRevision
	if statefulSet.Status.ReadyReplicas < desired || (upgrading && statefulSet.Status.UpdatedReplicas < desired) {
		reason := "Provisioning"
		if upgrading {
			reason = "Upgrading"
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, r.setNodeGroupStatus(ctx, group, cluster, false, reason, fmt.Sprintf("Waiting for node group: %d/%d ready.", statefulSet.Status.ReadyReplicas, desired), statefulSet.Status.ReadyReplicas)
	}
	return ctrl.Result{}, r.setNodeGroupStatus(ctx, group, cluster, true, "NodeGroupReady", fmt.Sprintf("Node group is ready: %d/%d nodes.", statefulSet.Status.ReadyReplicas, desired), statefulSet.Status.ReadyReplicas)
}

// resolveClusterTLS resolves the cluster's internal-TLS materials read-only (without creating
// cert-manager resources); the cluster controller owns provisioning. The TLS checksum is
// mirrored from the primary StatefulSet so the group rolls in lockstep on certificate rotation.
func (r *NiFiNodeGroupReconciler) resolveClusterTLS(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) (*clusterTLSMaterials, bool, error) {
	if !internalTLSEnabled(cluster) {
		return nil, true, nil
	}
	if cluster.Status.TLS == nil || !cluster.Status.TLS.Ready {
		return nil, false, nil
	}
	materials := resolveTLSPlan(cluster).materials()
	primary := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, primary); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, false, err
		}
	} else {
		materials.checksum = primary.Spec.Template.Annotations[managedTLSChecksumAnnotation]
	}
	return materials, true, nil
}

func (r *NiFiNodeGroupReconciler) reconcileNodeGroupStatefulSet(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup, tls *clusterTLSMaterials, replicas int32) (*appsv1.StatefulSet, error) {
	name := nodeGroupStatefulSetName(cluster, group)
	checksum := ""
	if tls != nil {
		checksum = tls.checksum
	}
	// The group's pods share the cluster's configuration overrides and authentication,
	// so their pod template carries the same checksums and rolls when either changes.
	resolvedOverrides, err := resolveConfigOverrides(ctx, r.Client, cluster)
	if err != nil {
		return nil, err
	}
	resolvedAuth, err := resolveClusterAuthentication(ctx, r.Client, cluster)
	if err != nil {
		return nil, err
	}
	if err := recreateOnClaimChange(ctx, r.Client, name, group.Namespace, desiredNodeGroupStatefulSetSpec(cluster, group, tls, replicas, checksum, resolvedOverrides.checksum, resolvedAuth).VolumeClaimTemplates); err != nil {
		return nil, err
	}
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: group.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		if statefulSet.ResourceVersion != "" && statefulSet.Annotations[nodeGroupAnnotation] != group.Name {
			return fmt.Errorf("StatefulSet %s/%s already exists and is not managed by NiFiNodeGroup %s", statefulSet.Namespace, statefulSet.Name, group.Name)
		}
		statefulSet.Labels = nodeGroupPodLabels(cluster, group)
		statefulSet.Annotations = map[string]string{nodeGroupAnnotation: group.Name}
		desired := desiredNodeGroupStatefulSetSpec(cluster, group, tls, replicas, checksum, resolvedOverrides.checksum, resolvedAuth)
		if statefulSet.ResourceVersion != "" {
			desired.ServiceName = statefulSet.Spec.ServiceName
			if statefulSet.Spec.Selector != nil {
				desired.Selector = statefulSet.Spec.Selector.DeepCopy()
			}
			desired.VolumeClaimTemplates = statefulSet.Spec.VolumeClaimTemplates
		}
		statefulSet.Spec = desired
		return controllerutil.SetControllerReference(group, statefulSet, r.Scheme)
	})
	return statefulSet, err
}

func (r *NiFiNodeGroupReconciler) reconcileNodeGroupDelete(ctx context.Context, group *nifiv1alpha1.NiFiNodeGroup) (ctrl.Result, error) {
	if group.Spec.DeletionPolicy == nifiv1alpha1.DeletionPolicyDelete {
		if name := group.Status.StatefulSetName; name != "" {
			sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: group.Namespace}}
			if err := r.Delete(ctx, sts); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
			pvcs := &corev1.PersistentVolumeClaimList{}
			if err := r.List(ctx, pvcs, client.InNamespace(group.Namespace), client.MatchingLabels{nodePoolLabel: group.Name}); err != nil {
				return ctrl.Result{}, err
			}
			for i := range pvcs.Items {
				if err := r.Delete(ctx, &pvcs.Items[i]); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}
			}
		}
	}
	_, err := removeFinalizer(ctx, r.Client, group)
	return ctrl.Result{}, err
}

func (r *NiFiNodeGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiNodeGroup{}).
		Owns(&appsv1.StatefulSet{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.nodeGroupsForCluster)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.nodeGroupsForOverridesSecret)).
		Complete(r)
}

// nodeGroupsForOverridesSecret re-enqueues node groups whose parent cluster sources
// configuration overrides from the changed Secret, so the group's pods roll on the new
// checksum like the primary pool.
func (r *NiFiNodeGroupReconciler) nodeGroupsForOverridesSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	groups := &nifiv1alpha1.NiFiNodeGroupList{}
	if err := r.List(ctx, groups, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range groups.Items {
		group := &groups.Items[i]
		cluster := &nifiv1alpha1.NiFiCluster{}
		if err := r.Get(ctx, types.NamespacedName{Name: group.Spec.ClusterRef.Name, Namespace: group.Namespace}, cluster); err != nil {
			continue
		}
		if cluster.Spec.ConfigOverrides == nil {
			continue
		}
		for _, reference := range cluster.Spec.ConfigOverrides.NiFiPropertiesFrom {
			if reference.Name == obj.GetName() {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: group.Name, Namespace: group.Namespace}})
				break
			}
		}
	}
	return requests
}

// nodeGroupsForCluster re-enqueues node groups when their parent cluster changes (for example
// when TLS becomes ready or certificates rotate).
func (r *NiFiNodeGroupReconciler) nodeGroupsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	groups := &nifiv1alpha1.NiFiNodeGroupList{}
	if err := r.List(ctx, groups, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range groups.Items {
		if groups.Items[i].Spec.ClusterRef.Name == obj.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: groups.Items[i].Name, Namespace: groups.Items[i].Namespace}})
		}
	}
	return requests
}

// setNodeGroupStatus updates the group status (including the scale subresource fields) only
// when something changed, so writing status does not retrigger reconcile in a tight loop.
func (r *NiFiNodeGroupReconciler) setNodeGroupStatus(ctx context.Context, group *nifiv1alpha1.NiFiNodeGroup, cluster *nifiv1alpha1.NiFiCluster, ready bool, reason, message string, replicas int32) error {
	selector := group.Status.Selector
	if cluster != nil {
		selector = nodeGroupScaleSelector(cluster, group)
	}
	if !nodeGroupStatusNeedsUpdate(group, ready, reason, selector, replicas) {
		return nil
	}
	if ready {
		group.Status.CommonStatus.MarkReady(group.Generation, reason, message)
		group.Status.Sync.LastError = ""
	} else {
		group.Status.CommonStatus.MarkNotReady(group.Generation, reason, message)
		group.Status.Dependencies.Ready = true
		group.Status.Dependencies.WaitingFor = nil
		group.Status.Sync.LastError = message
	}
	group.Status.Selector = selector
	group.Status.Replicas = replicas
	return r.Status().Update(ctx, group)
}

func (r *NiFiNodeGroupReconciler) markNodeGroupWaiting(ctx context.Context, group *nifiv1alpha1.NiFiNodeGroup, waitingFor []string) error {
	if group.Status.ObservedGeneration == group.Generation && !group.Status.Ready && !waitingForChanged(group.Status.Dependencies.WaitingFor, waitingFor) {
		return nil
	}
	group.Status.CommonStatus.MarkWaitingForDependencies(group.Generation, waitingFor)
	return r.Status().Update(ctx, group)
}

func nodeGroupStatusNeedsUpdate(group *nifiv1alpha1.NiFiNodeGroup, ready bool, reason, selector string, replicas int32) bool {
	if group.Status.ObservedGeneration != group.Generation || group.Status.Ready != ready ||
		group.Status.Selector != selector || group.Status.Replicas != replicas {
		return true
	}
	for _, condition := range group.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}
