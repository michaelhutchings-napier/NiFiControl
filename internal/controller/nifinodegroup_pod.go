package controller

import (
	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
)

const (
	// nodePoolLabel distinguishes a NiFiNodeGroup's pods from the cluster's primary pool and
	// from other groups, so each pool's StatefulSet selector is disjoint.
	nodePoolLabel = "nifi.controlnifi.io/node-pool"
	// nodeGroupAnnotation marks resources owned by a NiFiNodeGroup.
	nodeGroupAnnotation = "nifi.controlnifi.io/node-group"
)

// nodeGroupStatefulSetName is the StatefulSet name for a node group: <cluster>-nifi-<group>.
func nodeGroupStatefulSetName(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) string {
	return boundedManagedName(cluster.Name, "nifi-"+group.Name)
}

// nodeGroupPodLabels returns the cluster's pod labels (so the shared headless Service and
// PodDisruptionBudget select the group's pods) plus the pool label.
func nodeGroupPodLabels(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) map[string]string {
	podLabels := managedClusterPodLabels(cluster)
	podLabels[nodePoolLabel] = group.Name
	return podLabels
}

func nodeGroupScaleSelector(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) string {
	return labels.SelectorFromSet(nodeGroupPodLabels(cluster, group)).String()
}

func nodeGroupReplicas(group *nifiv1alpha1.NiFiNodeGroup) int32 {
	if group.Spec.Replicas > 0 {
		return group.Spec.Replicas
	}
	return 0
}

func nodeGroupImage(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) string {
	if group.Spec.Image != "" {
		return group.Spec.Image
	}
	return managedClusterImage(cluster)
}

func nodeGroupHeap(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) (string, string) {
	if group.Spec.JVM != nil {
		initial := group.Spec.JVM.HeapInitial
		if initial == "" {
			initial = "1g"
		}
		maximum := group.Spec.JVM.HeapMax
		if maximum == "" {
			maximum = "1g"
		}
		return initial, maximum
	}
	return managedClusterHeapInitial(cluster), managedClusterHeapMax(cluster)
}

func nodeGroupStorage(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) nifiv1alpha1.NiFiClusterStorageSpec {
	if group.Spec.Storage != nil {
		return *group.Spec.Storage
	}
	return cluster.Spec.Storage
}

func nodeGroupStorageEnabled(storage nifiv1alpha1.NiFiClusterStorageSpec) bool {
	return storage.Enabled == nil || *storage.Enabled
}

func nodeGroupScheduling(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) *nifiv1alpha1.NiFiClusterScheduling {
	if group.Spec.Scheduling != nil {
		return group.Spec.Scheduling
	}
	return cluster.Spec.Scheduling
}

func nodeGroupResources(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) corev1.ResourceRequirements {
	if len(group.Spec.Resources.Requests) > 0 || len(group.Spec.Resources.Limits) > 0 {
		return group.Spec.Resources
	}
	return cluster.Spec.Resources
}

func nodeGroupEnv(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup) []corev1.EnvVar {
	base := append([]corev1.EnvVar(nil), cluster.Spec.AdditionalEnv...)
	return mergeEnvironment(base, group.Spec.AdditionalEnv)
}

// desiredNodeGroupStatefulSetSpec builds the StatefulSet for a node group. The group's nodes
// join the cluster's headless Service and share its ZooKeeper, sensitive key, and TLS
// materials, so they are peers of the primary pool in one NiFi cluster. The pod construction
// reuses the same shared builders as the primary pool; only replicas, resources, JVM,
// storage, scheduling, and the update strategy vary per group.
func desiredNodeGroupStatefulSetSpec(cluster *nifiv1alpha1.NiFiCluster, group *nifiv1alpha1.NiFiNodeGroup, tls *clusterTLSMaterials, replicas int32, tlsChecksum, overridesChecksum string, auth *resolvedClusterAuth) appsv1.StatefulSetSpec {
	podLabels := nodeGroupPodLabels(cluster, group)
	webPort := defaultNiFiWebPort
	startCommand := managedNiFiStartCommand
	if tls != nil {
		webPort = tls.httpsPort
		startCommand = managedNiFiStartCommandTLS
	}
	heapInitial, heapMax := nodeGroupHeap(cluster, group)
	storage := nodeGroupStorage(cluster, group)

	container := corev1.Container{
		Name:            "nifi",
		Image:           nodeGroupImage(cluster, group),
		ImagePullPolicy: managedClusterImagePullPolicy(cluster),
		Command:         []string{"/bin/bash", "-ec", startCommand},
		// A node group only exists in a clustered cluster, so its nodes always join the cluster.
		Env: nodeEnvironment(cluster, tls, heapInitial, heapMax, nodeGroupEnv(cluster, group), true, auth),
		Ports: []corev1.ContainerPort{
			{Name: "web", ContainerPort: webPort, Protocol: corev1.ProtocolTCP},
			{Name: "cluster", ContainerPort: defaultClusterPort, Protocol: corev1.ProtocolTCP},
		},
		Resources:       nodeGroupResources(cluster, group),
		SecurityContext: managedClusterContainerSecurityContext(cluster),
		StartupProbe:    managedClusterStartupProbe(tls),
		LivenessProbe:   managedClusterLivenessProbe(tls),
		ReadinessProbe:  managedClusterReadinessProbe(tls),
		VolumeMounts:    managedClusterVolumeMounts(storage, tls, hasConfigOverrides(cluster), managedClusterAuthVolumeSource(cluster, auth) != ""),
	}
	podSpec := corev1.PodSpec{
		SecurityContext: managedClusterPodSecurityContext(cluster),
		InitContainers:  []corev1.Container{nodeDataInitializer(nodeGroupImage(cluster, group), managedClusterImagePullPolicy(cluster), managedClusterContainerSecurityContext(cluster))},
		Containers:      []corev1.Container{container},
		Volumes:         nodeVolumes(nodeGroupStorageEnabled(storage), tls, managedClusterOverridesVolumeSource(cluster), managedClusterAuthVolumeSource(cluster, auth)),
	}
	applyNodeScheduling(&podSpec, nodeGroupScheduling(cluster, group))

	annotations := map[string]string{nodeGroupAnnotation: group.Name}
	if tlsChecksum != "" {
		annotations[managedTLSChecksumAnnotation] = tlsChecksum
	}
	if overridesChecksum != "" {
		annotations[managedOverridesChecksumAnnotation] = overridesChecksum
	}
	if auth != nil && auth.checksum != "" {
		annotations[managedAuthChecksumAnnotation] = auth.checksum
	}
	upgrade := group.Spec.Upgrade
	spec := appsv1.StatefulSetSpec{
		ServiceName:          managedClusterHeadlessServiceName(cluster),
		Replicas:             ptr.To(replicas),
		RevisionHistoryLimit: ptr.To[int32](10),
		MinReadySeconds:      nodeMinReadySeconds(upgrade),
		PodManagementPolicy:  appsv1.ParallelPodManagement,
		Selector:             &metav1.LabelSelector{MatchLabels: podLabels},
		UpdateStrategy:       nodeUpdateStrategy(upgrade),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: podLabels, Annotations: annotations},
			Spec:       podSpec,
		},
	}
	applyPodCustomization(cluster, &spec.Template)
	if nodeGroupStorageEnabled(storage) {
		spec.VolumeClaimTemplates = nodeVolumeClaims(storage, podLabels, map[string]string{nodeGroupAnnotation: group.Name})
	}
	return spec
}
