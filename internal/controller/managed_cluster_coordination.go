package controller

import (
	"context"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// openShiftSCCAPIGroup and openShiftSCCResource identify the OpenShift
// SecurityContextConstraints resource that pods obtain through the 'use' verb.
const (
	openShiftSCCAPIGroup = "security.openshift.io"
	openShiftSCCResource = "securitycontextconstraints"
)

// managedClusterCoordinationServiceAccountName is the ServiceAccount the operator provisions
// for NiFi pods that need Kubernetes RBAC: Lease/ConfigMap access in Kubernetes coordination
// mode, and 'use' on an OpenShift SecurityContextConstraints when spec.pod.openShiftSCC is set.
func managedClusterCoordinationServiceAccountName(cluster *nifiv1alpha1.NiFiCluster) string {
	return managedClusterResourceName(cluster)
}

// managedClusterOpenShiftSCC returns the OpenShift SCC to grant the node pods, or "".
func managedClusterOpenShiftSCC(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.Pod != nil {
		return cluster.Spec.Pod.OpenShiftSCC
	}
	return ""
}

// managedClusterNeedsProvisionedRBAC reports whether the node pods need a ServiceAccount the
// operator binds Roles to — true for Kubernetes coordination mode or an OpenShift SCC grant.
func managedClusterNeedsProvisionedRBAC(cluster *nifiv1alpha1.NiFiCluster) bool {
	return managedClusterCoordinationMode(cluster) == nifiv1alpha1.CoordinationModeKubernetes ||
		managedClusterOpenShiftSCC(cluster) != ""
}

// managedClusterPodServiceAccountName resolves the ServiceAccount for the node pods: an
// explicit spec.pod.serviceAccountName wins; otherwise, when the pods need operator-managed
// RBAC (Kubernetes coordination or an OpenShift SCC grant), the operator-provisioned
// ServiceAccount is used; otherwise the namespace default.
func managedClusterPodServiceAccountName(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.Pod != nil && cluster.Spec.Pod.ServiceAccountName != "" {
		return cluster.Spec.Pod.ServiceAccountName
	}
	if managedClusterNeedsProvisionedRBAC(cluster) {
		return managedClusterCoordinationServiceAccountName(cluster)
	}
	return ""
}

// reconcileManagedClusterPodRBAC provisions the ServiceAccount and Role/RoleBindings the NiFi
// pods need: Lease/ConfigMap access for Kubernetes coordination mode, and 'use' on an
// OpenShift SCC when spec.pod.openShiftSCC is set. Each piece is created only when required
// and pruned otherwise, so switching modes cleans up.
func (r *NiFiClusterReconciler) reconcileManagedClusterPodRBAC(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	name := managedClusterCoordinationServiceAccountName(cluster)
	sccRBACName := name + "-scc"

	// ServiceAccount: provisioned only when RBAC is needed and the user has not supplied one.
	ownSA := managedClusterNeedsProvisionedRBAC(cluster) && (cluster.Spec.Pod == nil || cluster.Spec.Pod.ServiceAccountName == "")
	if ownSA {
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
			sa.Labels = managedClusterLabels(cluster)
			return controllerutil.SetControllerReference(cluster, sa, r.Scheme)
		}); err != nil {
			return err
		}
	} else if err := r.deleteManagedObject(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}); err != nil {
		return err
	}

	// Kubernetes coordination: leases + configmaps for leader election and the state provider.
	if managedClusterCoordinationMode(cluster) == nifiv1alpha1.CoordinationModeKubernetes {
		rules := []rbacv1.PolicyRule{
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
		}
		if err := r.reconcileManagedClusterRoleBinding(ctx, cluster, name, rules); err != nil {
			return err
		}
	} else if err := r.deleteManagedRoleBinding(ctx, cluster, name); err != nil {
		return err
	}

	// OpenShift SCC: 'use' on the named SecurityContextConstraints.
	if scc := managedClusterOpenShiftSCC(cluster); scc != "" {
		rules := []rbacv1.PolicyRule{{
			APIGroups:     []string{openShiftSCCAPIGroup},
			Resources:     []string{openShiftSCCResource},
			ResourceNames: []string{scc},
			Verbs:         []string{"use"},
		}}
		if err := r.reconcileManagedClusterRoleBinding(ctx, cluster, sccRBACName, rules); err != nil {
			return err
		}
	} else if err := r.deleteManagedRoleBinding(ctx, cluster, sccRBACName); err != nil {
		return err
	}
	return nil
}

// reconcileManagedClusterRoleBinding ensures a Role (with the given rules) and a RoleBinding
// tying it to the node pods' ServiceAccount.
func (r *NiFiClusterReconciler) reconcileManagedClusterRoleBinding(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, name string, rules []rbacv1.PolicyRule) error {
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels = managedClusterLabels(cluster)
		role.Rules = rules
		return controllerutil.SetControllerReference(cluster, role, r.Scheme)
	}); err != nil {
		return err
	}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		binding.Labels = managedClusterLabels(cluster)
		binding.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}
		binding.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      managedClusterPodServiceAccountName(cluster),
			Namespace: cluster.Namespace,
		}}
		return controllerutil.SetControllerReference(cluster, binding, r.Scheme)
	})
	return err
}

// deleteManagedRoleBinding removes a Role and its RoleBinding by name.
func (r *NiFiClusterReconciler) deleteManagedRoleBinding(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, name string) error {
	if err := r.deleteManagedObject(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}); err != nil {
		return err
	}
	return r.deleteManagedObject(ctx, &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}})
}

// deleteManagedObject deletes an object, ignoring not-found.
func (r *NiFiClusterReconciler) deleteManagedObject(ctx context.Context, obj client.Object) error {
	if err := r.Client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
