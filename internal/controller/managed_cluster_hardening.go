package controller

import (
	"context"
	"sort"
	"strings"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// applyManagedClusterScheduling threads pod placement controls onto the managed pod.
func applyManagedClusterScheduling(podSpec *corev1.PodSpec, cluster *nifiv1alpha1.NiFiCluster) {
	applyNodeScheduling(podSpec, cluster.Spec.Scheduling, oneNiFiNodePerNodeSelector(cluster))
}

// applyNodeScheduling threads pod placement controls onto any pool's pod. When
// oneNifiNodePerNode is set, a required host anti-affinity keyed on antiAffinityLabels is
// merged in so no two NiFi pods of the cluster share a node.
func applyNodeScheduling(podSpec *corev1.PodSpec, scheduling *nifiv1alpha1.NiFiClusterScheduling, antiAffinityLabels map[string]string) {
	if scheduling == nil {
		return
	}
	podSpec.NodeSelector = scheduling.NodeSelector
	podSpec.Tolerations = scheduling.Tolerations
	podSpec.Affinity = scheduling.Affinity
	podSpec.TopologySpreadConstraints = scheduling.TopologySpreadConstraints
	podSpec.PriorityClassName = scheduling.PriorityClassName
	if scheduling.OneNiFiNodePerNode {
		podSpec.Affinity = withHostAntiAffinity(podSpec.Affinity, antiAffinityLabels)
	}
}

// oneNiFiNodePerNodeSelector labels the NiFi pods of one cluster (both the primary pool and
// all NiFiNodeGroup pools carry managedClusterLabel), so the anti-affinity spreads the whole
// cluster.
func oneNiFiNodePerNodeSelector(cluster *nifiv1alpha1.NiFiCluster) map[string]string {
	return map[string]string{managedClusterLabel: managedClusterResourceName(cluster)}
}

// withHostAntiAffinity returns a copy of affinity with a required pod anti-affinity term added
// that keeps pods matching selectorLabels off the same node. Any existing affinity
// (nodeAffinity, podAffinity, other podAntiAffinity terms) is preserved, and the input is not
// mutated so it is safe to call on the cached spec each reconcile.
func withHostAntiAffinity(affinity *corev1.Affinity, selectorLabels map[string]string) *corev1.Affinity {
	out := &corev1.Affinity{}
	if affinity != nil {
		out = affinity.DeepCopy()
	}
	if out.PodAntiAffinity == nil {
		out.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}
	out.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		out.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
		corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{MatchLabels: selectorLabels},
			TopologyKey:   "kubernetes.io/hostname",
		},
	)
	return out
}

// managedClusterUpdateStrategy resolves the StatefulSet update strategy from spec.upgrade.
func managedClusterUpdateStrategy(cluster *nifiv1alpha1.NiFiCluster) appsv1.StatefulSetUpdateStrategy {
	return nodeUpdateStrategy(cluster.Spec.Upgrade)
}

// nodeUpdateStrategy resolves the StatefulSet update strategy for any pool.
func nodeUpdateStrategy(upgrade *nifiv1alpha1.NiFiClusterUpgradeSpec) appsv1.StatefulSetUpdateStrategy {
	if upgrade != nil && upgrade.Strategy == "OnDelete" {
		return appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}
	}
	strategy := appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}
	if upgrade != nil && upgrade.Partition != nil {
		strategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: upgrade.Partition}
	}
	return strategy
}

func managedClusterMinReadySeconds(cluster *nifiv1alpha1.NiFiCluster) int32 {
	return nodeMinReadySeconds(cluster.Spec.Upgrade)
}

func nodeMinReadySeconds(upgrade *nifiv1alpha1.NiFiClusterUpgradeSpec) int32 {
	if upgrade != nil {
		return upgrade.MinReadySeconds
	}
	return 0
}

// managedClusterProxyHost returns the comma-separated nifi.web.proxy.host allow-list,
// combining the TLS Service DNS names (when TLS is enabled) with any Ingress host.
func managedClusterProxyHost(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials) string {
	hosts := map[string]struct{}{}
	if tls != nil && tls.proxyHosts != "" {
		for _, host := range strings.Split(tls.proxyHosts, ",") {
			if host != "" {
				hosts[host] = struct{}{}
			}
		}
	}
	if ingress := cluster.Spec.Ingress; ingress != nil && ingress.Enabled && ingress.Host != "" {
		hosts[ingress.Host] = struct{}{}
	}
	// User-supplied additive entries: external load balancers or DNS names people reach
	// NiFi through that the operator cannot infer from the Service or Ingress.
	for _, host := range cluster.Spec.AdditionalProxyHosts {
		if trimmed := strings.TrimSpace(string(host)); trimmed != "" {
			hosts[trimmed] = struct{}{}
		}
	}
	if len(hosts) == 0 {
		return ""
	}
	ordered := make([]string, 0, len(hosts))
	for host := range hosts {
		ordered = append(ordered, host)
	}
	sort.Strings(ordered)
	return strings.Join(ordered, ",")
}

func managedClusterProxyContextPath(cluster *nifiv1alpha1.NiFiCluster) string {
	if ingress := cluster.Spec.Ingress; ingress != nil {
		return ingress.ContextPath
	}
	return ""
}

// reconcileManagedClusterPDB creates, updates, or removes the cluster PodDisruptionBudget.
func (r *NiFiClusterReconciler) reconcileManagedClusterPDB(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}}
	spec := cluster.Spec.PodDisruptionBudget
	if spec == nil || !spec.Enabled {
		return r.deleteManagedClusterResource(ctx, cluster, pdb)
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := assertManagedClusterResource(pdb, cluster); err != nil {
			return err
		}
		pdb.Labels = managedClusterLabels(cluster)
		pdb.Annotations = managedClusterAnnotations(cluster)
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: managedClusterPodLabels(cluster)}
		switch {
		case spec.MaxUnavailable != nil:
			pdb.Spec.MaxUnavailable = spec.MaxUnavailable
			pdb.Spec.MinAvailable = nil
		case spec.MinAvailable != nil:
			pdb.Spec.MinAvailable = spec.MinAvailable
			pdb.Spec.MaxUnavailable = nil
		default:
			pdb.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(1))
			pdb.Spec.MinAvailable = nil
		}
		return nil
	})
	return err
}

// reconcileManagedClusterIngress creates, updates, or removes the cluster Ingress.
func (r *NiFiClusterReconciler) reconcileManagedClusterIngress(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	ingress := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}}
	spec := cluster.Spec.Ingress
	if spec == nil || !spec.Enabled {
		return r.deleteManagedClusterResource(ctx, cluster, ingress)
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ingress, func() error {
		if err := assertManagedClusterResource(ingress, cluster); err != nil {
			return err
		}
		ingress.Labels = managedClusterLabels(cluster)
		ingress.Annotations = managedClusterIngressAnnotations(cluster)
		if spec.IngressClassName != "" {
			ingress.Spec.IngressClassName = ptr.To(spec.IngressClassName)
		} else {
			ingress.Spec.IngressClassName = nil
		}
		path := spec.Path
		if path == "" {
			path = "/"
		}
		pathType := networkingv1.PathType(spec.PathType)
		if pathType == "" {
			pathType = networkingv1.PathTypePrefix
		}
		ingress.Spec.Rules = []networkingv1.IngressRule{{
			Host: spec.Host,
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path:     path,
						PathType: ptr.To(pathType),
						Backend: networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{
								Name: managedClusterResourceName(cluster),
								Port: networkingv1.ServiceBackendPort{Number: managedClusterServicePort(cluster)},
							},
						},
					}},
				},
			},
		}}
		if spec.TLS != nil {
			hosts := spec.TLS.Hosts
			if len(hosts) == 0 {
				hosts = []string{spec.Host}
			}
			ingress.Spec.TLS = []networkingv1.IngressTLS{{Hosts: hosts, SecretName: spec.TLS.SecretName}}
		} else {
			ingress.Spec.TLS = nil
		}
		return nil
	})
	return err
}

func managedClusterIngressAnnotations(cluster *nifiv1alpha1.NiFiCluster) map[string]string {
	annotations := map[string]string{}
	if cluster.Spec.Ingress != nil {
		for key, value := range cluster.Spec.Ingress.Annotations {
			if key == managedClusterAnnotation {
				continue
			}
			annotations[key] = value
		}
	}
	annotations[managedClusterAnnotation] = cluster.Name
	return annotations
}
