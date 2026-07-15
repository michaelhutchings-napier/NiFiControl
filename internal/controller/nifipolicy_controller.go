package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifipolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifipolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifipolicies/finalizers,verbs=update

// NiFiPolicyReconciler manages NiFi access policies. It resolves the referenced NiFiUser and
// NiFiUserGroup tenants to their NiFi ids and ensures a policy for the (resource, action) tuple
// grants exactly those tenants. Access policies require a secured NiFi with a managed authorizer.
type NiFiPolicyReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	AccessPolicyClient nifi.AccessPolicyClient
}

func (r *NiFiPolicyReconciler) accessPolicyClient() nifi.AccessPolicyClient {
	if r.AccessPolicyClient != nil {
		return r.AccessPolicyClient
	}
	return nifi.HTTPAccessPolicyClient{}
}

func (r *NiFiPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiPolicy{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcilePolicyDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, r.policyDependenciesWaitingFor(ctx, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markPolicyWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkPolicyNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markPolicyNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// NiFi permits exactly one access policy per (resource, action), so two NiFiPolicy CRs
	// targeting the same tuple would fight over its tenant list. Enforce a single, deterministic
	// owner; the losers report a conflict instead of overwriting each other's grants.
	if owner := r.conflictingPolicyOwner(ctx, instance); owner != "" {
		message := fmt.Sprintf("NiFiPolicy %q already manages the %q policy for %q; NiFi permits one access policy per (resource, action). Consolidate the userRefs/userGroupRefs into a single NiFiPolicy.", owner, instance.Spec.Action, instance.Spec.Resource)
		if shouldMarkPolicyNotReady(instance, "PolicyConflict", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markPolicyNotReady(ctx, r.Client, instance, "PolicyConflict", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	userIDs, groupIDs, err := r.resolvePolicyTenants(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	desired := nifi.AccessPolicyComponent{
		Resource:   instance.Spec.Resource,
		Action:     instance.Spec.Action,
		Users:      tenantRefs(userIDs),
		UserGroups: tenantRefs(groupIDs),
	}
	policies := r.accessPolicyClient()

	// Resolve the exact policy: by recorded id, then by (action, resource), else create.
	if instance.Status.NiFiID != "" {
		existing, err := policies.GetAccessPolicy(ctx, endpoint, instance.Status.NiFiID)
		if err != nil && !nifi.IsNotFound(err) {
			return r.policyGetFailed(ctx, instance, err)
		}
		if existing != nil {
			return r.reconcileExistingPolicy(ctx, instance, endpoint, policies, existing, desired, userIDs, groupIDs)
		}
	}

	existing, err := policies.GetAccessPolicyForResource(ctx, endpoint, instance.Spec.Action, instance.Spec.Resource)
	if err != nil && !nifi.IsNotFound(err) {
		return r.policyGetFailed(ctx, instance, err)
	}
	// A non-404 response may be an inherited (effective) policy for an ancestor resource; only
	// treat it as the exact policy when the resource and action match.
	if existing != nil && existing.Component.Resource == instance.Spec.Resource && existing.Component.Action == instance.Spec.Action {
		return r.reconcileExistingPolicy(ctx, instance, endpoint, policies, existing, desired, userIDs, groupIDs)
	}

	created, err := policies.CreateAccessPolicy(ctx, endpoint, nifi.AccessPolicyEntity{Revision: nifi.Revision{Version: 0}, Component: desired})
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi access policy: %v", err)
		if shouldMarkPolicyNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markPolicyNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, markPolicyReady(ctx, r.Client, instance, nifi.AccessPolicyEntityID(*created), created.Revision.Version, userIDs, groupIDs)
}

func (r *NiFiPolicyReconciler) reconcileExistingPolicy(ctx context.Context, instance *nifiv1alpha1.NiFiPolicy, endpoint string, policies nifi.AccessPolicyClient, existing *nifi.AccessPolicyEntity, desired nifi.AccessPolicyComponent, userIDs, groupIDs []string) (ctrl.Result, error) {
	nifiID := nifi.AccessPolicyEntityID(*existing)
	if policyNeedsUpdate(*existing, desired) {
		update := nifi.AccessPolicyEntity{
			Revision:  existing.Revision,
			ID:        nifiID,
			Component: nifi.AccessPolicyComponent{ID: nifiID, Resource: desired.Resource, Action: desired.Action, Users: desired.Users, UserGroups: desired.UserGroups},
		}
		updated, err := policies.UpdateAccessPolicy(ctx, endpoint, update)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi access policy: %v", err)
			if shouldMarkPolicyNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markPolicyNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			return ctrl.Result{}, markPolicyReady(ctx, r.Client, instance, nifi.AccessPolicyEntityID(*updated), updated.Revision.Version, userIDs, groupIDs)
		}
	}
	if !policyStatusMatches(instance, nifiID, existing.Revision.Version, userIDs, groupIDs) {
		return ctrl.Result{}, markPolicyReady(ctx, r.Client, instance, nifiID, existing.Revision.Version, userIDs, groupIDs)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiPolicyReconciler) policyGetFailed(ctx context.Context, instance *nifiv1alpha1.NiFiPolicy, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to get NiFi access policy: %v", err)
	if shouldMarkPolicyNotReady(instance, "NiFiGetFailed", message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markPolicyNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *NiFiPolicyReconciler) reconcilePolicyDelete(ctx context.Context, instance *nifiv1alpha1.NiFiPolicy) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and its NiFi access policy) is gone; nothing to delete remotely.
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
	if err := r.accessPolicyClient().DeleteAccessPolicy(ctx, endpoint, instance.Status.NiFiID, instance.Status.Revision.Version); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

// policyDependenciesWaitingFor reports referenced tenants that are missing or not yet Ready.
func (r *NiFiPolicyReconciler) policyDependenciesWaitingFor(ctx context.Context, instance *nifiv1alpha1.NiFiPolicy) []string {
	waitingFor := make([]string, 0)
	for _, ref := range instance.Spec.UserRefs {
		namespace := localObjectRefNamespace(instance.Namespace, ref)
		user := &nifiv1alpha1.NiFiUser{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: namespace}, user); err != nil {
			if apierrors.IsNotFound(err) {
				waitingFor = append(waitingFor, fmt.Sprintf("NiFiUser/%s/%s", namespace, ref.Name))
				continue
			}
			waitingFor = append(waitingFor, fmt.Sprintf("NiFiUser/%s/%s:GetError", namespace, ref.Name))
			continue
		}
		if !user.Status.Ready || user.Status.NiFiID == "" {
			waitingFor = append(waitingFor, fmt.Sprintf("NiFiUser/%s/%s:Ready", namespace, ref.Name))
		}
	}
	for _, ref := range instance.Spec.UserGroupRefs {
		namespace := localObjectRefNamespace(instance.Namespace, ref)
		group := &nifiv1alpha1.NiFiUserGroup{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: namespace}, group); err != nil {
			if apierrors.IsNotFound(err) {
				waitingFor = append(waitingFor, fmt.Sprintf("NiFiUserGroup/%s/%s", namespace, ref.Name))
				continue
			}
			waitingFor = append(waitingFor, fmt.Sprintf("NiFiUserGroup/%s/%s:GetError", namespace, ref.Name))
			continue
		}
		if !group.Status.Ready || group.Status.NiFiID == "" {
			waitingFor = append(waitingFor, fmt.Sprintf("NiFiUserGroup/%s/%s:Ready", namespace, ref.Name))
		}
	}
	return waitingFor
}

func (r *NiFiPolicyReconciler) resolvePolicyTenants(ctx context.Context, instance *nifiv1alpha1.NiFiPolicy) (userIDs, groupIDs []string, err error) {
	for _, ref := range instance.Spec.UserRefs {
		user := &nifiv1alpha1.NiFiUser{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(instance.Namespace, ref)}, user); err != nil {
			return nil, nil, err
		}
		if user.Status.NiFiID != "" {
			userIDs = append(userIDs, user.Status.NiFiID)
		}
	}
	for _, ref := range instance.Spec.UserGroupRefs {
		group := &nifiv1alpha1.NiFiUserGroup{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(instance.Namespace, ref)}, group); err != nil {
			return nil, nil, err
		}
		if group.Status.NiFiID != "" {
			groupIDs = append(groupIDs, group.Status.NiFiID)
		}
	}
	return userIDs, groupIDs, nil
}

func policyNeedsUpdate(existing nifi.AccessPolicyEntity, desired nifi.AccessPolicyComponent) bool {
	return !sameTenantSet(existing.Component.Users, desired.Users) || !sameTenantSet(existing.Component.UserGroups, desired.UserGroups)
}

// policyTupleKey identifies the single NiFi access policy a NiFiPolicy targets: its resolved
// cluster plus the (action, resource) tuple. Two NiFiPolicy CRs sharing a key contend for the
// same NiFi policy.
func policyTupleKey(p *nifiv1alpha1.NiFiPolicy) string {
	clusterNamespace := clusterRefNamespace(p.Namespace, p.Spec.ClusterRef)
	return fmt.Sprintf("%s/%s|%s|%s", clusterNamespace, p.Spec.ClusterRef.Name, p.Spec.Action, p.Spec.Resource)
}

// samePolicy reports whether two NiFiPolicy objects are the same CR (namespaced name is unique).
func samePolicy(a, b *nifiv1alpha1.NiFiPolicy) bool {
	return a.Namespace == b.Namespace && a.Name == b.Name
}

// policyPrecedes gives a total, reconcile-order-independent ordering over policies contending for
// a tuple: oldest first, then by namespace/name. Every controller instance therefore agrees on
// the single owner.
func policyPrecedes(a, b *nifiv1alpha1.NiFiPolicy) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	return a.Name < b.Name
}

// conflictingPolicyOwner returns the "namespace/name" of the NiFiPolicy that owns instance's
// (cluster, resource, action) tuple when instance is not itself that owner, or "" when instance
// is the sole/owning claimant. A transient list error fails open (returns "") so it never blocks
// reconciliation on its own.
func (r *NiFiPolicyReconciler) conflictingPolicyOwner(ctx context.Context, instance *nifiv1alpha1.NiFiPolicy) string {
	list := &nifiv1alpha1.NiFiPolicyList{}
	if err := r.List(ctx, list); err != nil {
		return ""
	}
	key := policyTupleKey(instance)
	owner := instance
	for i := range list.Items {
		other := &list.Items[i]
		if samePolicy(other, instance) || !other.DeletionTimestamp.IsZero() {
			continue
		}
		if policyTupleKey(other) == key && policyPrecedes(other, owner) {
			owner = other
		}
	}
	if samePolicy(owner, instance) {
		return ""
	}
	return fmt.Sprintf("%s/%s", owner.Namespace, owner.Name)
}

func (r *NiFiPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiPolicy{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPolicyCluster)).
		Watches(&nifiv1alpha1.NiFiUser{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPolicyUser)).
		Watches(&nifiv1alpha1.NiFiUserGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForPolicyUserGroup)).
		Complete(r)
}

// allPolicies lists every NiFiPolicy across all namespaces. The dependency watches must scan all
// namespaces, not just the changed object's own: a NiFiPolicy may reference a NiFiUser,
// NiFiUserGroup, or NiFiCluster in a different namespace (LocalObjectReference and ClusterReference
// both carry a namespace). Because the reconcile does not requeue while waiting on a dependency, a
// namespace-scoped list would never wake a cross-namespace policy when its dependency becomes Ready,
// leaving it stuck until the next periodic resync.
func (r *NiFiPolicyReconciler) allPolicies(ctx context.Context) []nifiv1alpha1.NiFiPolicy {
	list := &nifiv1alpha1.NiFiPolicyList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	return list.Items
}

func (r *NiFiPolicyReconciler) requestsForPolicyCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	var out []reconcile.Request
	for _, policy := range r.allPolicies(ctx) {
		if policy.Spec.ClusterRef.Name == obj.GetName() && clusterRefNamespace(policy.Namespace, policy.Spec.ClusterRef) == obj.GetNamespace() {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policy.Namespace}})
		}
	}
	return out
}

func (r *NiFiPolicyReconciler) requestsForPolicyUser(ctx context.Context, obj client.Object) []reconcile.Request {
	var out []reconcile.Request
	for _, policy := range r.allPolicies(ctx) {
		for _, ref := range policy.Spec.UserRefs {
			if ref.Name == obj.GetName() && localObjectRefNamespace(policy.Namespace, ref) == obj.GetNamespace() {
				out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policy.Namespace}})
				break
			}
		}
	}
	return out
}

func (r *NiFiPolicyReconciler) requestsForPolicyUserGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	var out []reconcile.Request
	for _, policy := range r.allPolicies(ctx) {
		for _, ref := range policy.Spec.UserGroupRefs {
			if ref.Name == obj.GetName() && localObjectRefNamespace(policy.Namespace, ref) == obj.GetNamespace() {
				out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: policy.Name, Namespace: policy.Namespace}})
				break
			}
		}
	}
	return out
}

// --- status helpers ---------------------------------------------------------

func markPolicyReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiPolicy, nifiID string, revisionVersion int64, userIDs, groupIDs []string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "PolicyReady", "The NiFi access policy is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.UserIDs = userIDs
	obj.Status.UserGroupIDs = groupIDs
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markPolicyNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiPolicy, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markPolicyWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiPolicy, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func policyStatusMatches(obj *nifiv1alpha1.NiFiPolicy, nifiID string, revisionVersion int64, userIDs, groupIDs []string) bool {
	return obj.Status.ObservedGeneration == obj.Generation &&
		obj.Status.Ready &&
		obj.Status.Dependencies.Ready &&
		obj.Status.NiFiID == nifiID &&
		obj.Status.Revision.Version == revisionVersion &&
		sameStringSet(obj.Status.UserIDs, userIDs) &&
		sameStringSet(obj.Status.UserGroupIDs, groupIDs)
}

func shouldMarkPolicyNotReady(obj *nifiv1alpha1.NiFiPolicy, reason, message string) bool {
	if obj.Status.ObservedGeneration != obj.Generation || obj.Status.Ready || obj.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range obj.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}
