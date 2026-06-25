package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDesiredManagedClusterStatefulSetSchedulingAndUpgrade(t *testing.T) {
	partition := int32(2)
	cluster := &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "dataflows"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 3,
			Coordination: &nifiv1alpha1.NiFiClusterCoordinationSpec{
				ZooKeeperConnectString: "zk.dataflows.svc:2181",
			},
			Scheduling: &nifiv1alpha1.NiFiClusterScheduling{
				NodeSelector:      map[string]string{"disktype": "ssd"},
				PriorityClassName: "high",
				Tolerations:       []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew: 1, TopologyKey: "kubernetes.io/hostname", WhenUnsatisfiable: corev1.DoNotSchedule,
				}},
			},
			Upgrade: &nifiv1alpha1.NiFiClusterUpgradeSpec{Strategy: "RollingUpdate", Partition: &partition, MinReadySeconds: 20},
		},
	}
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil)
	if spec.Template.Spec.NodeSelector["disktype"] != "ssd" {
		t.Fatalf("nodeSelector = %#v", spec.Template.Spec.NodeSelector)
	}
	if spec.Template.Spec.PriorityClassName != "high" {
		t.Fatalf("priorityClassName = %q", spec.Template.Spec.PriorityClassName)
	}
	if len(spec.Template.Spec.Tolerations) != 1 || len(spec.Template.Spec.TopologySpreadConstraints) != 1 {
		t.Fatalf("scheduling not applied: %#v", spec.Template.Spec)
	}
	if spec.MinReadySeconds != 20 {
		t.Fatalf("minReadySeconds = %d, want 20", spec.MinReadySeconds)
	}
	if spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType ||
		spec.UpdateStrategy.RollingUpdate == nil || spec.UpdateStrategy.RollingUpdate.Partition == nil ||
		*spec.UpdateStrategy.RollingUpdate.Partition != 2 {
		t.Fatalf("update strategy = %#v", spec.UpdateStrategy)
	}

	cluster.Spec.Upgrade.Strategy = "OnDelete"
	if got := managedClusterUpdateStrategy(cluster); got.Type != appsv1.OnDeleteStatefulSetStrategyType {
		t.Fatalf("OnDelete strategy = %#v", got)
	}
}

func hardeningCluster() *nifiv1alpha1.NiFiCluster {
	storage := false
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:     nifiv1alpha1.ClusterModeInternal,
			Replicas: 1,
			Storage:  nifiv1alpha1.NiFiClusterStorageSpec{Enabled: &storage},
		},
	}
}

func TestManagedClusterPDBReconcile(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := hardeningCluster()
	maxUnavailable := intstr.FromInt32(1)
	cluster.Spec.PodDisruptionBudget = &nifiv1alpha1.NiFiClusterPDBSpec{Enabled: true, MaxUnavailable: &maxUnavailable}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterPDB(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	pdb := &policyv1.PodDisruptionBudget{}
	key := types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, pdb); err != nil {
		t.Fatal(err)
	}
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("maxUnavailable = %#v", pdb.Spec.MaxUnavailable)
	}
	if pdb.Spec.Selector == nil || pdb.Spec.Selector.MatchLabels[managedClusterLabel] == "" {
		t.Fatalf("selector = %#v", pdb.Spec.Selector)
	}

	// Disabling removes the PDB.
	cluster.Spec.PodDisruptionBudget.Enabled = false
	if err := r.reconcileManagedClusterPDB(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(context.Background(), key, pdb); !apierrors.IsNotFound(err) {
		t.Fatalf("PDB should be deleted when disabled, err=%v", err)
	}
}

func TestManagedClusterPDBDefaultsToMaxUnavailableOne(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := hardeningCluster()
	cluster.Spec.PodDisruptionBudget = &nifiv1alpha1.NiFiClusterPDBSpec{Enabled: true}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	if err := r.reconcileManagedClusterPDB(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	pdb := &policyv1.PodDisruptionBudget{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, pdb); err != nil {
		t.Fatal(err)
	}
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 || pdb.Spec.MinAvailable != nil {
		t.Fatalf("default PDB = %#v / %#v", pdb.Spec.MaxUnavailable, pdb.Spec.MinAvailable)
	}
}

func TestManagedClusterIngressReconcileAndProxyHost(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := hardeningCluster()
	cluster.Spec.Ingress = &nifiv1alpha1.NiFiClusterIngressSpec{
		Enabled:          true,
		IngressClassName: "nginx",
		Host:             "nifi.example.com",
		Path:             "/",
		PathType:         "Prefix",
		Annotations:      map[string]string{"a": "b"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterIngress(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	ingress := &networkingv1.Ingress{}
	key := types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, ingress); err != nil {
		t.Fatal(err)
	}
	if ingress.Spec.IngressClassName == nil || *ingress.Spec.IngressClassName != "nginx" {
		t.Fatalf("ingressClassName = %#v", ingress.Spec.IngressClassName)
	}
	if len(ingress.Spec.Rules) != 1 || ingress.Spec.Rules[0].Host != "nifi.example.com" {
		t.Fatalf("rules = %#v", ingress.Spec.Rules)
	}
	backend := ingress.Spec.Rules[0].HTTP.Paths[0].Backend.Service
	if backend == nil || backend.Name != managedClusterResourceName(cluster) || backend.Port.Number != defaultNiFiWebPort {
		t.Fatalf("backend = %#v", backend)
	}

	// The ingress host is added to the allowed proxy hosts, and the HTTP start command sets it.
	env := managedClusterEnvironment(cluster, nil)
	assertEnvironmentValue(t, env, "NIFI_WEB_PROXY_HOST", "nifi.example.com")
	if !strings.Contains(managedNiFiStartCommand, "nifi.web.proxy.host") {
		t.Fatal("HTTP start command must configure nifi.web.proxy.host")
	}

	// Disabling removes the Ingress.
	cluster.Spec.Ingress.Enabled = false
	if err := r.reconcileManagedClusterIngress(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(context.Background(), key, ingress); !apierrors.IsNotFound(err) {
		t.Fatalf("Ingress should be deleted when disabled, err=%v", err)
	}
}

func TestManagedClusterReconcileCreatesPDBAndIngress(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := hardeningCluster()
	maxUnavailable := intstr.FromInt32(1)
	cluster.Spec.PodDisruptionBudget = &nifiv1alpha1.NiFiClusterPDBSpec{Enabled: true, MaxUnavailable: &maxUnavailable}
	cluster.Spec.Ingress = &nifiv1alpha1.NiFiClusterIngressSpec{Enabled: true, Host: "nifi.example.com"}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &appsv1.StatefulSet{}).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme, ReachabilityChecker: fakeReachabilityChecker{}}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	for range 2 {
		if _, err := r.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	key := types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, &policyv1.PodDisruptionBudget{}); err != nil {
		t.Fatalf("PDB not created by reconcile: %v", err)
	}
	if err := k8sClient.Get(context.Background(), key, &networkingv1.Ingress{}); err != nil {
		t.Fatalf("Ingress not created by reconcile: %v", err)
	}
}
