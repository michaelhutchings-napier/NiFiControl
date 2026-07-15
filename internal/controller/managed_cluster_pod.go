package controller

import (
	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// applyPodCustomization overlays spec.pod onto a generated node pod template. Operator-
// managed metadata wins: user labels and annotations are added only where the operator
// has not set the key, so selector labels and configuration checksums are preserved.
// Extra volumes, NiFi-container mounts, init containers, and sidecars are appended after
// the operator's own; reserved volume and container names are rejected at admission.
func applyPodCustomization(cluster *nifiv1alpha1.NiFiCluster, template *corev1.PodTemplateSpec) {
	pod := cluster.Spec.Pod
	if pod == nil {
		return
	}
	if len(pod.Labels) > 0 && template.Labels == nil {
		template.Labels = map[string]string{}
	}
	for key, value := range pod.Labels {
		if _, exists := template.Labels[key]; !exists {
			template.Labels[key] = value
		}
	}
	if len(pod.Annotations) > 0 && template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	for key, value := range pod.Annotations {
		if _, exists := template.Annotations[key]; !exists {
			template.Annotations[key] = value
		}
	}
	template.Spec.ImagePullSecrets = append(template.Spec.ImagePullSecrets, pod.ImagePullSecrets...)
	if pod.ServiceAccountName != "" {
		template.Spec.ServiceAccountName = pod.ServiceAccountName
	}
	template.Spec.Volumes = append(template.Spec.Volumes, pod.ExtraVolumes...)
	if len(pod.ExtraVolumeMounts) > 0 && len(template.Spec.Containers) > 0 {
		template.Spec.Containers[0].VolumeMounts = append(template.Spec.Containers[0].VolumeMounts, pod.ExtraVolumeMounts...)
	}
	template.Spec.InitContainers = append(template.Spec.InitContainers, pod.ExtraInitContainers...)
	template.Spec.Containers = append(template.Spec.Containers, pod.ExtraContainers...)
	if pod.ShareProcessNamespace != nil {
		template.Spec.ShareProcessNamespace = pod.ShareProcessNamespace
	}
}
