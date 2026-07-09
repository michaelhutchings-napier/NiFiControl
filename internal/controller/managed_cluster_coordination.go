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

// managedClusterCoordinationServiceAccountName is the ServiceAccount the operator provisions
// for NiFi pods in Kubernetes coordination mode; it is granted the Lease and ConfigMap
// permissions the KubernetesLeaderElectionManager and KubernetesConfigMapStateProvider need.
func managedClusterCoordinationServiceAccountName(cluster *nifiv1alpha1.NiFiCluster) string {
	return managedClusterResourceName(cluster)
}

// managedClusterPodServiceAccountName resolves the ServiceAccount for the node pods: an
// explicit spec.pod.serviceAccountName wins; otherwise Kubernetes coordination mode uses the
// operator-provisioned coordination ServiceAccount; otherwise the namespace default is used.
func managedClusterPodServiceAccountName(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.Pod != nil && cluster.Spec.Pod.ServiceAccountName != "" {
		return cluster.Spec.Pod.ServiceAccountName
	}
	if managedClusterCoordinationMode(cluster) == nifiv1alpha1.CoordinationModeKubernetes {
		return managedClusterCoordinationServiceAccountName(cluster)
	}
	return ""
}

// reconcileManagedClusterCoordinationRBAC provisions the ServiceAccount, Role, and
// RoleBinding a NiFi cluster needs to coordinate through Kubernetes (Lease-based leader
// election plus the ConfigMap state provider). It runs only in Kubernetes coordination mode;
// in ZooKeeper mode it prunes any RBAC left from a previous Kubernetes-mode configuration.
func (r *NiFiClusterReconciler) reconcileManagedClusterCoordinationRBAC(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	name := managedClusterCoordinationServiceAccountName(cluster)
	if managedClusterCoordinationMode(cluster) != nifiv1alpha1.CoordinationModeKubernetes {
		return r.pruneManagedClusterCoordinationRBAC(ctx, cluster, name)
	}

	// Provision the ServiceAccount ourselves only when the user did not supply one; a
	// user-provided spec.pod.serviceAccountName is theirs to own, but we still bind the
	// coordination Role to whatever the pods actually run as.
	if cluster.Spec.Pod == nil || cluster.Spec.Pod.ServiceAccountName == "" {
		serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, serviceAccount, func() error {
			serviceAccount.Labels = managedClusterLabels(cluster)
			return controllerutil.SetControllerReference(cluster, serviceAccount, r.Scheme)
		}); err != nil {
			return err
		}
	}

	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, role, func() error {
		role.Labels = managedClusterLabels(cluster)
		role.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{"coordination.k8s.io"},
				Resources: []string{"leases"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		}
		return controllerutil.SetControllerReference(cluster, role, r.Scheme)
	}); err != nil {
		return err
	}

	roleBinding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, roleBinding, func() error {
		roleBinding.Labels = managedClusterLabels(cluster)
		roleBinding.RoleRef = rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}
		roleBinding.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      managedClusterPodServiceAccountName(cluster),
			Namespace: cluster.Namespace,
		}}
		return controllerutil.SetControllerReference(cluster, roleBinding, r.Scheme)
	})
	return err
}

// pruneManagedClusterCoordinationRBAC removes the coordination RBAC objects (used when the
// cluster is not, or is no longer, in Kubernetes coordination mode).
func (r *NiFiClusterReconciler) pruneManagedClusterCoordinationRBAC(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, name string) error {
	objs := []client.Object{
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}},
	}
	for _, obj := range objs {
		if err := r.Client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
