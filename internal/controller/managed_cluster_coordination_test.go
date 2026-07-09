package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func envValue(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func clusteredKubernetesCluster() *nifiv1alpha1.NiFiCluster {
	cluster := hardeningCluster()
	cluster.Spec.Replicas = 2
	cluster.Spec.Coordination = &nifiv1alpha1.NiFiClusterCoordinationSpec{Mode: nifiv1alpha1.CoordinationModeKubernetes}
	return cluster
}

func TestManagedClusterCoordinationMode(t *testing.T) {
	cluster := hardeningCluster()
	if got := managedClusterCoordinationMode(cluster); got != nifiv1alpha1.CoordinationModeZooKeeper {
		t.Fatalf("default mode = %q, want ZooKeeper", got)
	}
	cluster.Spec.Coordination = &nifiv1alpha1.NiFiClusterCoordinationSpec{Mode: nifiv1alpha1.CoordinationModeKubernetes}
	if got := managedClusterCoordinationMode(cluster); got != nifiv1alpha1.CoordinationModeKubernetes {
		t.Fatalf("mode = %q, want Kubernetes", got)
	}
}

func TestManagedClusterCoordinationConfigError(t *testing.T) {
	// No coordination at all: invalid for a clustered NiFi.
	cluster := hardeningCluster()
	if msg := managedClusterCoordinationConfigError(cluster); msg == "" {
		t.Fatal("expected an error when spec.coordination is nil")
	}
	// ZooKeeper mode without a connect string: invalid.
	cluster.Spec.Coordination = &nifiv1alpha1.NiFiClusterCoordinationSpec{Mode: nifiv1alpha1.CoordinationModeZooKeeper}
	if msg := managedClusterCoordinationConfigError(cluster); msg == "" {
		t.Fatal("expected an error for ZooKeeper mode without a connect string")
	}
	// ZooKeeper mode with a connect string: valid.
	cluster.Spec.Coordination.ZooKeeperConnectString = "zk.default.svc:2181"
	if msg := managedClusterCoordinationConfigError(cluster); msg != "" {
		t.Fatalf("unexpected error for valid ZooKeeper config: %s", msg)
	}
	// Kubernetes mode: valid with no ZooKeeper at all.
	cluster.Spec.Coordination = &nifiv1alpha1.NiFiClusterCoordinationSpec{Mode: nifiv1alpha1.CoordinationModeKubernetes}
	if msg := managedClusterCoordinationConfigError(cluster); msg != "" {
		t.Fatalf("Kubernetes mode should not require ZooKeeper: %s", msg)
	}
}

func TestManagedClusterCoordinationEnv(t *testing.T) {
	// Kubernetes mode: selects the ConfigMap provider + Lease leader election, no ZooKeeper.
	cluster := clusteredKubernetesCluster()
	env := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil).Template.Spec.Containers[0].Env
	if v, _ := envValue(env, "NIFI_CLUSTER_STATE_PROVIDER"); v != "kubernetes-provider" {
		t.Fatalf("state provider = %q, want kubernetes-provider", v)
	}
	if v, _ := envValue(env, "NIFI_LEADER_ELECTION_IMPL"); v != "KubernetesLeaderElectionManager" {
		t.Fatalf("leader election = %q, want KubernetesLeaderElectionManager", v)
	}
	if v, _ := envValue(env, "NIFI_K8S_LEASE_PREFIX"); v != managedClusterResourceName(cluster) {
		t.Fatalf("lease prefix = %q, want %q", v, managedClusterResourceName(cluster))
	}
	if _, ok := envValue(env, "NIFI_ZK_CONNECT_STRING"); ok {
		t.Fatal("Kubernetes mode must not set NIFI_ZK_CONNECT_STRING")
	}

	// ZooKeeper mode: sets the connect string, no Kubernetes coordination env.
	zk := hardeningCluster()
	zk.Spec.Replicas = 2
	zk.Spec.Coordination = &nifiv1alpha1.NiFiClusterCoordinationSpec{ZooKeeperConnectString: "zk.default.svc:2181"}
	env = desiredManagedClusterStatefulSetSpec(zk, nil, "", nil).Template.Spec.Containers[0].Env
	if v, _ := envValue(env, "NIFI_ZK_CONNECT_STRING"); v != "zk.default.svc:2181" {
		t.Fatalf("zk connect string = %q", v)
	}
	if _, ok := envValue(env, "NIFI_K8S_LEASE_PREFIX"); ok {
		t.Fatal("ZooKeeper mode must not set NIFI_K8S_LEASE_PREFIX")
	}
}

func TestManagedClusterPodServiceAccountName(t *testing.T) {
	// ZooKeeper mode: namespace default (empty).
	zk := hardeningCluster()
	if got := managedClusterPodServiceAccountName(zk); got != "" {
		t.Fatalf("ZooKeeper mode SA = %q, want empty", got)
	}
	// Kubernetes mode: operator-provisioned coordination SA.
	k8s := clusteredKubernetesCluster()
	if got := managedClusterPodServiceAccountName(k8s); got != managedClusterCoordinationServiceAccountName(k8s) {
		t.Fatalf("Kubernetes mode SA = %q, want %q", got, managedClusterCoordinationServiceAccountName(k8s))
	}
	// An explicit spec.pod.serviceAccountName always wins.
	k8s.Spec.Pod = &nifiv1alpha1.NiFiClusterPodSpec{ServiceAccountName: "byo-sa"}
	if got := managedClusterPodServiceAccountName(k8s); got != "byo-sa" {
		t.Fatalf("user SA not honored: %q", got)
	}
	// And the pod template carries it.
	sa := desiredManagedClusterStatefulSetSpec(k8s, nil, "", nil).Template.Spec.ServiceAccountName
	if sa != "byo-sa" {
		t.Fatalf("pod ServiceAccountName = %q, want byo-sa", sa)
	}
}

func TestManagedClusterCoordinationRBACReconcileAndPrune(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := clusteredKubernetesCluster()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	ctx := context.Background()
	name := managedClusterCoordinationServiceAccountName(cluster)
	key := types.NamespacedName{Name: name, Namespace: cluster.Namespace}

	// Kubernetes mode: provision the ServiceAccount, Role, and RoleBinding.
	if err := r.reconcileManagedClusterCoordinationRBAC(ctx, cluster); err != nil {
		t.Fatalf("reconcile (Kubernetes mode): %v", err)
	}
	if err := k8sClient.Get(ctx, key, &corev1.ServiceAccount{}); err != nil {
		t.Fatalf("ServiceAccount not created: %v", err)
	}
	role := &rbacv1.Role{}
	if err := k8sClient.Get(ctx, key, role); err != nil {
		t.Fatalf("Role not created: %v", err)
	}
	if !roleGrants(role, "coordination.k8s.io", "leases") || !roleGrants(role, "", "configmaps") {
		t.Fatalf("Role missing leases/configmaps grant: %#v", role.Rules)
	}
	binding := &rbacv1.RoleBinding{}
	if err := k8sClient.Get(ctx, key, binding); err != nil {
		t.Fatalf("RoleBinding not created: %v", err)
	}
	if binding.RoleRef.Name != name || len(binding.Subjects) != 1 || binding.Subjects[0].Name != name {
		t.Fatalf("RoleBinding wrong ref/subject: %#v / %#v", binding.RoleRef, binding.Subjects)
	}

	// Reverting to ZooKeeper mode prunes the RBAC.
	cluster.Spec.Coordination.Mode = nifiv1alpha1.CoordinationModeZooKeeper
	cluster.Spec.Coordination.ZooKeeperConnectString = "zk.default.svc:2181"
	if err := r.reconcileManagedClusterCoordinationRBAC(ctx, cluster); err != nil {
		t.Fatalf("reconcile (ZooKeeper mode): %v", err)
	}
	if err := k8sClient.Get(ctx, key, &rbacv1.RoleBinding{}); !apierrors.IsNotFound(err) {
		t.Fatalf("RoleBinding not pruned: %v", err)
	}
	if err := k8sClient.Get(ctx, key, &rbacv1.Role{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Role not pruned: %v", err)
	}
	if err := k8sClient.Get(ctx, key, &corev1.ServiceAccount{}); !apierrors.IsNotFound(err) {
		t.Fatalf("ServiceAccount not pruned: %v", err)
	}
}

func roleGrants(role *rbacv1.Role, group, resource string) bool {
	for _, rule := range role.Rules {
		hasGroup := false
		for _, g := range rule.APIGroups {
			if g == group {
				hasGroup = true
			}
		}
		if !hasGroup {
			continue
		}
		for _, res := range rule.Resources {
			if res == resource {
				return true
			}
		}
	}
	return false
}
