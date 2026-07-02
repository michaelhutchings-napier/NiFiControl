package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiremoteprocessgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiremoteprocessgroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiremoteprocessgroups/finalizers,verbs=update

// NiFiRemoteProcessGroupReconciler manages NiFi remote process groups (site-to-site links). It
// creates the RPG under its parent process group, keeps its configuration (target URIs, transport,
// timeouts, proxy) in sync, and stops transmission before any config update or deletion because
// NiFi refuses to edit or remove a transmitting RPG. Transmission is not a declarative field in
// v1alpha1: enabling it requires wired-up remote ports, so the operator leaves the RPG stopped and
// only reports the observed transmission state in status.
type NiFiRemoteProcessGroupReconciler struct {
	client.Client
	Scheme                   *runtime.Scheme
	RemoteProcessGroupClient nifi.RemoteProcessGroupClient
}

func (r *NiFiRemoteProcessGroupReconciler) remoteProcessGroupClient() nifi.RemoteProcessGroupClient {
	if r.RemoteProcessGroupClient != nil {
		return r.RemoteProcessGroupClient
	}
	return nifi.HTTPRemoteProcessGroupClient{}
}

func (r *NiFiRemoteProcessGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileRemoteProcessGroupDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, remoteProcessGroupDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markRemoteProcessGroupWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkRemoteProcessGroupNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markRemoteProcessGroupNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkRemoteProcessGroupNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markRemoteProcessGroupNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	desired, secretWaiting, err := r.desiredRemoteProcessGroup(ctx, instance, parentID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(secretWaiting) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, secretWaiting) {
			return ctrl.Result{}, markRemoteProcessGroupWaitingForDependencies(ctx, r.Client, instance, secretWaiting)
		}
		return ctrl.Result{}, nil
	}

	rpgs := r.remoteProcessGroupClient()
	if instance.Status.NiFiID != "" {
		existing, err := rpgs.GetRemoteProcessGroup(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi remote process group: %v", err)
			if shouldMarkRemoteProcessGroupNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markRemoteProcessGroupNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingRemoteProcessGroup(ctx, instance, endpoint, rpgs, desired, existing, parentID)
	}

	created, err := rpgs.CreateRemoteProcessGroup(ctx, endpoint, parentID, desired)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi remote process group: %v", err)
		if shouldMarkRemoteProcessGroupNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markRemoteProcessGroupNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty remote process group response."
		if shouldMarkRemoteProcessGroupNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markRemoteProcessGroupNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := nifi.RemoteProcessGroupEntityID(*created)
	if !remoteProcessGroupStatusMatches(instance, nifiID, created.Revision.Version, parentID, created.Component) {
		return ctrl.Result{}, markRemoteProcessGroupReady(ctx, r.Client, instance, nifiID, created.Revision.Version, parentID, created.Component)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiRemoteProcessGroupReconciler) reconcileExistingRemoteProcessGroup(ctx context.Context, instance *nifiv1alpha1.NiFiRemoteProcessGroup, endpoint string, rpgs nifi.RemoteProcessGroupClient, desired nifi.RemoteProcessGroupEntity, existing *nifi.RemoteProcessGroupEntity, parentID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty remote process group response."
		if shouldMarkRemoteProcessGroupNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markRemoteProcessGroupNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := nifi.RemoteProcessGroupEntityID(*existing)
	current := existing

	if remoteProcessGroupNeedsUpdate(desired, *current) {
		// NiFi refuses to edit a transmitting RPG; stop transmission first.
		if current.Component.Transmitting {
			stopped, err := rpgs.UpdateRemoteProcessGroupRunStatus(ctx, endpoint, nifiID, current.Revision.Version, "STOPPED")
			if err != nil {
				return r.remoteProcessGroupWriteFailed(ctx, instance, "NiFiUpdateFailed", "stop", err)
			}
			if stopped != nil {
				current = stopped
			}
		}
		update := desired
		update.ID = nifiID
		update.Component.ID = nifiID
		update.Revision.Version = current.Revision.Version
		updated, err := rpgs.UpdateRemoteProcessGroup(ctx, endpoint, update)
		if err != nil {
			return r.remoteProcessGroupWriteFailed(ctx, instance, "NiFiUpdateFailed", "update", err)
		}
		if updated != nil {
			current = updated
		}
	}

	if !remoteProcessGroupStatusMatches(instance, nifiID, current.Revision.Version, parentID, current.Component) {
		return ctrl.Result{}, markRemoteProcessGroupReady(ctx, r.Client, instance, nifiID, current.Revision.Version, parentID, current.Component)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiRemoteProcessGroupReconciler) remoteProcessGroupWriteFailed(ctx context.Context, instance *nifiv1alpha1.NiFiRemoteProcessGroup, reason, verb string, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to %s NiFi remote process group: %v", verb, err)
	if shouldMarkRemoteProcessGroupNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markRemoteProcessGroupNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *NiFiRemoteProcessGroupReconciler) reconcileRemoteProcessGroupDelete(ctx context.Context, instance *nifiv1alpha1.NiFiRemoteProcessGroup) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and its remote process group) is gone; nothing to delete remotely.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	rpgs := r.remoteProcessGroupClient()
	// A transmitting RPG cannot be deleted; read it for the current revision, stop it if needed,
	// then delete.
	current, err := rpgs.GetRemoteProcessGroup(ctx, endpoint, instance.Status.NiFiID)
	if err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	if current != nil {
		revision := current.Revision.Version
		if current.Component.Transmitting {
			stopped, err := rpgs.UpdateRemoteProcessGroupRunStatus(ctx, endpoint, instance.Status.NiFiID, revision, "STOPPED")
			if err != nil {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, err
			}
			if stopped != nil {
				revision = stopped.Revision.Version
			}
		}
		if err := rpgs.DeleteRemoteProcessGroup(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiRemoteProcessGroupReconciler) desiredRemoteProcessGroup(ctx context.Context, instance *nifiv1alpha1.NiFiRemoteProcessGroup, parentID string) (nifi.RemoteProcessGroupEntity, []string, error) {
	component := nifi.RemoteProcessGroupComponent{
		ParentGroupID:         parentID,
		Name:                  instance.Name,
		Comments:              instance.Spec.Comments,
		TargetURIs:            strings.Join(instance.Spec.TargetURIs, ","),
		TransportProtocol:     instance.Spec.TransportProtocol,
		CommunicationsTimeout: instance.Spec.CommunicationsTimeout,
		YieldDuration:         instance.Spec.YieldDuration,
		LocalNetworkInterface: instance.Spec.LocalNetworkInterface,
	}
	if instance.Spec.Position != nil {
		component.Position = &nifi.Position{X: float64(instance.Spec.Position.X), Y: float64(instance.Spec.Position.Y)}
	}
	waitingFor := make([]string, 0)
	if instance.Spec.Proxy != nil {
		component.ProxyHost = instance.Spec.Proxy.Host
		component.ProxyPort = instance.Spec.Proxy.Port
		component.ProxyUser = instance.Spec.Proxy.User
		if instance.Spec.Proxy.PasswordSecretRef != nil {
			value, waiting, err := r.resolveRemoteProcessGroupSecret(ctx, instance.Namespace, instance.Spec.Proxy.PasswordSecretRef)
			if err != nil {
				return nifi.RemoteProcessGroupEntity{}, nil, err
			}
			if waiting != "" {
				waitingFor = append(waitingFor, waiting)
			}
			component.ProxyPassword = value
		}
	}
	return nifi.RemoteProcessGroupEntity{Revision: nifi.Revision{Version: 0}, Component: component}, waitingFor, nil
}

func (r *NiFiRemoteProcessGroupReconciler) resolveRemoteProcessGroupSecret(ctx context.Context, namespace string, ref *nifiv1alpha1.SecretKeyRef) (string, string, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Sprintf("Secret/%s/%s", namespace, ref.Name), nil
		}
		return "", "", err
	}
	data, ok := secret.Data[ref.Key]
	if !ok {
		if ref.Optional != nil && *ref.Optional {
			return "", "", nil
		}
		return "", fmt.Sprintf("Secret/%s/%s:%s", namespace, ref.Name, ref.Key), nil
	}
	return string(data), "", nil
}

func (r *NiFiRemoteProcessGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiRemoteProcessGroup{}, clusterRefIndexField, indexRemoteProcessGroupClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiRemoteProcessGroup{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiRemoteProcessGroupReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiRemoteProcessGroupList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiRemoteProcessGroupReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiRemoteProcessGroupList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func indexRemoteProcessGroupClusterRef(obj client.Object) []string {
	rpg, ok := obj.(*nifiv1alpha1.NiFiRemoteProcessGroup)
	if !ok {
		return nil
	}
	return indexClusterRef(rpg.Namespace, rpg.Spec.ClusterRef)
}

func remoteProcessGroupDependenciesWaitingFor(ctx context.Context, c client.Client, rpg *nifiv1alpha1.NiFiRemoteProcessGroup) []string {
	return processGroupReferenceDependencyWaitingFor(ctx, c, rpg.Namespace, rpg.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
}

func remoteProcessGroupNeedsUpdate(desired nifi.RemoteProcessGroupEntity, existing nifi.RemoteProcessGroupEntity) bool {
	if desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID {
		return true
	}
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Comments != existing.Component.Comments ||
		desired.Component.TransportProtocol != existing.Component.TransportProtocol ||
		desired.Component.CommunicationsTimeout != existing.Component.CommunicationsTimeout ||
		desired.Component.YieldDuration != existing.Component.YieldDuration ||
		desired.Component.LocalNetworkInterface != existing.Component.LocalNetworkInterface ||
		desired.Component.ProxyHost != existing.Component.ProxyHost ||
		desired.Component.ProxyPort != existing.Component.ProxyPort ||
		desired.Component.ProxyUser != existing.Component.ProxyUser {
		return true
	}
	if !targetURIsEqual(desired.Component.TargetURIs, existing.Component.TargetURIs) {
		return true
	}
	return !nifiPositionsEqual(desired.Component.Position, existing.Component.Position)
}

// targetURIsEqual compares two comma-separated target URI lists as sets, so ordering and
// surrounding whitespace do not trigger perpetual drift.
func targetURIsEqual(left, right string) bool {
	normalize := func(value string) []string {
		parts := strings.Split(value, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
		sort.Strings(out)
		return out
	}
	l, r := normalize(left), normalize(right)
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i] != r[i] {
			return false
		}
	}
	return true
}

func remoteProcessGroupTransmissionStatus(transmitting bool) string {
	if transmitting {
		return "Transmitting"
	}
	return "Stopped"
}

func remoteProcessGroupStatusMatches(instance *nifiv1alpha1.NiFiRemoteProcessGroup, nifiID string, revisionVersion int64, parentID string, component nifi.RemoteProcessGroupComponent) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ParentProcessGroupID == parentID &&
		instance.Status.TransmissionStatus == remoteProcessGroupTransmissionStatus(component.Transmitting) &&
		instance.Status.TargetSecure == component.TargetSecure &&
		instance.Status.InputPortCount == component.InputPortCount &&
		instance.Status.OutputPortCount == component.OutputPortCount
}

func shouldMarkRemoteProcessGroupNotReady(instance *nifiv1alpha1.NiFiRemoteProcessGroup, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func markRemoteProcessGroupReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiRemoteProcessGroup, nifiID string, revisionVersion int64, parentProcessGroupID string, component nifi.RemoteProcessGroupComponent) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "RemoteProcessGroupReady", "The NiFi remote process group is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ParentProcessGroupID = parentProcessGroupID
	obj.Status.TransmissionStatus = remoteProcessGroupTransmissionStatus(component.Transmitting)
	obj.Status.TargetSecure = component.TargetSecure
	obj.Status.InputPortCount = component.InputPortCount
	obj.Status.OutputPortCount = component.OutputPortCount
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markRemoteProcessGroupNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiRemoteProcessGroup, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markRemoteProcessGroupWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiRemoteProcessGroup, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}
