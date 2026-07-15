package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func podTestCluster(pod *nifiv1alpha1.NiFiClusterPodSpec) *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "edge", Namespace: "default"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode: nifiv1alpha1.ClusterModeInternal,
			Pod:  pod,
		},
	}
}

func TestApplyPodCustomizationAppendsWithoutClobberingOperatorMetadata(t *testing.T) {
	cluster := podTestCluster(&nifiv1alpha1.NiFiClusterPodSpec{
		Labels:             map[string]string{"team": "data", managedClusterLabel: "hijacked"},
		Annotations:        map[string]string{"example.com/scrape": "true"},
		ImagePullSecrets:   []corev1.LocalObjectReference{{Name: "regcred"}},
		ServiceAccountName: "nifi-nodes",
		ExtraVolumes:       []corev1.Volume{{Name: "nars", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
		ExtraVolumeMounts:  []corev1.VolumeMount{{Name: "nars", MountPath: "/opt/nifi/nifi-current/nar_extensions"}},
		ExtraInitContainers: []corev1.Container{
			{Name: "fetch-nars", Image: "busybox:1.36"},
		},
		ExtraContainers: []corev1.Container{
			{Name: "log-shipper", Image: "busybox:1.36"},
		},
	})
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	template := spec.Template

	if template.Labels["team"] != "data" {
		t.Fatal("expected the custom pod label to be added")
	}
	if got := template.Labels[managedClusterLabel]; got == "hijacked" {
		t.Fatal("operator-managed pod label was overridden by spec.pod.labels")
	}
	if template.Annotations["example.com/scrape"] != "true" {
		t.Fatal("expected the custom pod annotation to be added")
	}
	if len(template.Spec.ImagePullSecrets) != 1 || template.Spec.ImagePullSecrets[0].Name != "regcred" {
		t.Fatalf("expected imagePullSecrets to be applied, got %+v", template.Spec.ImagePullSecrets)
	}
	if template.Spec.ServiceAccountName != "nifi-nodes" {
		t.Fatalf("expected serviceAccountName nifi-nodes, got %q", template.Spec.ServiceAccountName)
	}
	if last := template.Spec.Volumes[len(template.Spec.Volumes)-1]; last.Name != "nars" {
		t.Fatalf("expected the extra volume to be appended last, got %q", last.Name)
	}
	nifiMounts := template.Spec.Containers[0].VolumeMounts
	if last := nifiMounts[len(nifiMounts)-1]; last.Name != "nars" || last.MountPath != "/opt/nifi/nifi-current/nar_extensions" {
		t.Fatalf("expected the extra mount on the NiFi container, got %+v", last)
	}
	if len(template.Spec.InitContainers) != 2 || template.Spec.InitContainers[1].Name != "fetch-nars" {
		t.Fatalf("expected the extra init container after the data initializer, got %+v", template.Spec.InitContainers)
	}
	if len(template.Spec.Containers) != 2 || template.Spec.Containers[1].Name != "log-shipper" {
		t.Fatalf("expected the sidecar after the NiFi container, got %+v", template.Spec.Containers)
	}
	// The selector must still match the (unclobbered) pod labels.
	for key, value := range spec.Selector.MatchLabels {
		if template.Labels[key] != value {
			t.Fatalf("pod label %s=%s no longer matches the selector", key, template.Labels[key])
		}
	}
}

func TestApplyPodCustomizationNoopWithoutSpecPod(t *testing.T) {
	spec := desiredManagedClusterStatefulSetSpec(podTestCluster(nil), nil, "", nil)
	if len(spec.Template.Spec.Containers) != 1 || len(spec.Template.Spec.InitContainers) != 1 {
		t.Fatal("expected only the operator's containers without spec.pod")
	}
	if spec.Template.Spec.ServiceAccountName != "" {
		t.Fatal("expected no serviceAccountName without spec.pod")
	}
}

func TestNodeGroupPodsInheritPodCustomization(t *testing.T) {
	cluster := podTestCluster(&nifiv1alpha1.NiFiClusterPodSpec{
		ExtraContainers: []corev1.Container{{Name: "log-shipper", Image: "busybox:1.36"}},
	})
	group := &nifiv1alpha1.NiFiNodeGroup{ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "default"}}
	spec := desiredNodeGroupStatefulSetSpec(cluster, group, nil, 1, "", "", nil)
	if len(spec.Template.Spec.Containers) != 2 || spec.Template.Spec.Containers[1].Name != "log-shipper" {
		t.Fatal("expected node group pods to carry the cluster's sidecar")
	}
}

func TestLogbackOverrideRendersAndDrivesChecksum(t *testing.T) {
	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		LogbackXml: `<configuration><root level="WARN"/></configuration>`,
	})
	if !hasConfigOverrides(cluster) {
		t.Fatal("expected a logback-only override to count as configOverrides")
	}
	resolved, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.data[overridesLogbackKey] != cluster.Spec.ConfigOverrides.LogbackXml {
		t.Fatalf("expected logback.xml in the resolved payload, got %v", resolved.data)
	}
	if resolved.checksum == "" {
		t.Fatal("expected a checksum for a logback-only override")
	}
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	command := strings.Join(spec.Template.Spec.Containers[0].Command, "\n")
	if !strings.Contains(command, "logback.xml.image-default") {
		t.Fatal("expected the start command to restore logback.xml from the image default when the override is removed")
	}
}

func TestPodShareProcessNamespace(t *testing.T) {
	tmpl := desiredManagedClusterStatefulSetSpec(podTestCluster(&nifiv1alpha1.NiFiClusterPodSpec{ShareProcessNamespace: ptr.To(true)}), nil, "", nil).Template
	if tmpl.Spec.ShareProcessNamespace == nil || !*tmpl.Spec.ShareProcessNamespace {
		t.Fatalf("expected shareProcessNamespace=true on the pod spec, got %v", tmpl.Spec.ShareProcessNamespace)
	}
	def := desiredManagedClusterStatefulSetSpec(podTestCluster(&nifiv1alpha1.NiFiClusterPodSpec{}), nil, "", nil).Template
	if def.Spec.ShareProcessNamespace != nil {
		t.Fatalf("shareProcessNamespace should be unset by default, got %v", *def.Spec.ShareProcessNamespace)
	}
}

func TestPodSuspendOnCrashHoldsContainerAndDropsRestartProbes(t *testing.T) {
	c := desiredManagedClusterStatefulSetSpec(podTestCluster(&nifiv1alpha1.NiFiClusterPodSpec{SuspendOnCrash: ptr.To(true)}), nil, "", nil).Template.Spec.Containers[0]
	cmd := c.Command[len(c.Command)-1]
	if strings.Contains(cmd, `exec "${NIFI_HOME}/bin/nifi.sh" run`) {
		t.Error("suspendOnCrash should replace the exec run so the shell survives a NiFi crash")
	}
	if !strings.Contains(cmd, "sleep infinity") {
		t.Error("suspendOnCrash should hold the container with sleep infinity after NiFi exits")
	}
	if c.LivenessProbe != nil || c.StartupProbe != nil {
		t.Error("suspendOnCrash must drop the liveness and startup probes so the held container is not restarted")
	}
	if c.ReadinessProbe == nil {
		t.Error("suspendOnCrash should keep the readiness probe so the suspended pod reports NotReady")
	}
}

func TestPodSuspendOnCrashOffKeepsExecAndProbes(t *testing.T) {
	c := desiredManagedClusterStatefulSetSpec(podTestCluster(&nifiv1alpha1.NiFiClusterPodSpec{}), nil, "", nil).Template.Spec.Containers[0]
	if !strings.Contains(c.Command[len(c.Command)-1], `exec "${NIFI_HOME}/bin/nifi.sh" run`) {
		t.Error("default start command should exec nifi.sh run")
	}
	if c.LivenessProbe == nil || c.StartupProbe == nil {
		t.Error("liveness and startup probes should be present by default")
	}
}
