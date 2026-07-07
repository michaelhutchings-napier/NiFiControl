package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func overridesTestCluster(overrides *nifiv1alpha1.NiFiClusterConfigOverrides) *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "edge", Namespace: "default"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:            nifiv1alpha1.ClusterModeInternal,
			ConfigOverrides: overrides,
		},
	}
}

func overridesTestClient(objects ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func TestResolveConfigOverridesSortsAndSplitsFiles(t *testing.T) {
	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		NiFiProperties: map[string]nifiv1alpha1.ConfigOverrideValue{
			"nifi.queue.swap.threshold": "15000",
			"custom.prop":               "hello=world|x$y",
		},
		BootstrapProperties: map[string]nifiv1alpha1.ConfigOverrideValue{"java.arg.debug": "-XX:+PrintFlagsFinal"},
	})
	resolved, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if got := resolved.data[overridesNiFiPropertiesKey]; got != "custom.prop=hello=world|x$y\nnifi.queue.swap.threshold=15000\n" {
		t.Fatalf("unexpected nifi.properties rendering: %q", got)
	}
	if got := resolved.data[overridesBootstrapKey]; got != "java.arg.debug=-XX:+PrintFlagsFinal\n" {
		t.Fatalf("unexpected bootstrap.conf rendering: %q", got)
	}
	if resolved.checksum == "" {
		t.Fatal("expected a checksum")
	}
	if _, ok := resolved.data[overridesLogbackKey]; ok {
		t.Fatal("expected no logback.xml entry without logbackXml")
	}

	again, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster)
	if err != nil || again.checksum != resolved.checksum {
		t.Fatalf("checksum not stable: %v %s vs %s", err, again.checksum, resolved.checksum)
	}
	cluster.Spec.ConfigOverrides.NiFiProperties["custom.prop"] = "changed"
	changed, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster)
	if err != nil || changed.checksum == resolved.checksum {
		t.Fatalf("checksum did not change with an override value: %v", err)
	}

	empty, err := resolveConfigOverrides(context.Background(), overridesTestClient(), overridesTestCluster(nil))
	if err != nil || !empty.empty() || empty.checksum != "" {
		t.Fatalf("expected empty resolution without overrides, got %+v (%v)", empty, err)
	}
}

func TestResolveConfigOverridesMergesSecretsWithInlinePrecedence(t *testing.T) {
	first := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ldap-conf", Namespace: "default"},
		Data: map[string][]byte{
			"nifi.administrative.yield.duration": []byte("20 sec"),
			"custom.from.secret":                 []byte("one"),
		},
	}
	second := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "tuning", Namespace: "default"},
		Data:       map[string][]byte{"custom.from.secret": []byte("two")},
	}
	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		NiFiProperties:     map[string]nifiv1alpha1.ConfigOverrideValue{"nifi.administrative.yield.duration": "30 sec"},
		NiFiPropertiesFrom: []corev1.LocalObjectReference{{Name: "ldap-conf"}, {Name: "tuning"}},
	})
	resolved, err := resolveConfigOverrides(context.Background(), overridesTestClient(first, second), cluster)
	if err != nil {
		t.Fatal(err)
	}
	body := resolved.data[overridesNiFiPropertiesKey]
	// Later Secrets win over earlier ones; inline entries win over Secrets.
	if !strings.Contains(body, "custom.from.secret=two\n") {
		t.Fatalf("expected the later Secret to win, got %q", body)
	}
	if !strings.Contains(body, "nifi.administrative.yield.duration=30 sec\n") {
		t.Fatalf("expected the inline entry to win over the Secret, got %q", body)
	}
}

func TestResolveConfigOverridesRejectsInvalidSecretEntries(t *testing.T) {
	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		NiFiPropertiesFrom: []corev1.LocalObjectReference{{Name: "sneaky"}},
	})

	denylisted := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sneaky", Namespace: "default"},
		Data:       map[string][]byte{"nifi.web.https.port": []byte("9999")},
	}
	if _, err := resolveConfigOverrides(context.Background(), overridesTestClient(denylisted), cluster); err == nil {
		t.Fatal("expected a denylisted Secret-sourced property to be rejected")
	}

	newline := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sneaky", Namespace: "default"},
		Data:       map[string][]byte{"custom.multiline": []byte("a\nnifi.web.https.port=9999")},
	}
	if _, err := resolveConfigOverrides(context.Background(), overridesTestClient(newline), cluster); err == nil {
		t.Fatal("expected a newline value to be rejected")
	}

	if _, err := resolveConfigOverrides(context.Background(), overridesTestClient(), cluster); err == nil {
		t.Fatal("expected a missing Secret to be an error")
	}
}

func TestDesiredManagedClusterStatefulSetWiresConfigOverrides(t *testing.T) {
	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		NiFiProperties: map[string]nifiv1alpha1.ConfigOverrideValue{"nifi.queue.swap.threshold": "15000"},
	})
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "payload-checksum", nil)

	foundVolume := false
	for _, volume := range spec.Template.Spec.Volumes {
		if volume.Name == managedOverridesVolume {
			foundVolume = true
			if volume.Secret == nil || volume.Secret.SecretName != managedClusterOverridesSecretName(cluster) {
				t.Fatalf("overrides volume does not reference the overrides Secret: %+v", volume)
			}
		}
	}
	if !foundVolume {
		t.Fatal("expected an overrides Secret volume")
	}
	foundMount := false
	for _, mount := range spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == managedOverridesVolume && mount.MountPath == managedOverridesDir && mount.ReadOnly {
			foundMount = true
		}
	}
	if !foundMount {
		t.Fatal("expected a read-only overrides volume mount")
	}
	if spec.Template.Annotations[managedOverridesChecksumAnnotation] != "payload-checksum" {
		t.Fatal("expected the overrides checksum pod-template annotation")
	}

	command := strings.Join(spec.Template.Spec.Containers[0].Command, "\n")
	if !strings.Contains(command, "apply_config_overrides") {
		t.Fatal("expected the start command to apply configuration overrides")
	}
	initCommand := strings.Join(spec.Template.Spec.InitContainers[0].Command, "\n")
	if !strings.Contains(initCommand, "nifi.properties.image-default") || !strings.Contains(initCommand, "bootstrap.conf.image-default") {
		t.Fatal("expected the data initializer to capture image-default configuration copies")
	}
}

func TestDesiredManagedClusterStatefulSetOmitsOverridesWhenUnset(t *testing.T) {
	spec := desiredManagedClusterStatefulSetSpec(overridesTestCluster(nil), nil, "", nil)
	for _, volume := range spec.Template.Spec.Volumes {
		if volume.Name == managedOverridesVolume {
			t.Fatal("expected no overrides volume without spec.configOverrides")
		}
	}
	if _, ok := spec.Template.Annotations[managedOverridesChecksumAnnotation]; ok {
		t.Fatal("expected no overrides checksum annotation without spec.configOverrides")
	}
}

func TestSecretOnlyOverridesStillMountTheOverridesVolume(t *testing.T) {
	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		NiFiPropertiesFrom: []corev1.LocalObjectReference{{Name: "tuning"}},
	})
	if !hasConfigOverrides(cluster) {
		t.Fatal("expected Secret-only overrides to count as configOverrides")
	}
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "sum", nil)
	found := false
	for _, volume := range spec.Template.Spec.Volumes {
		if volume.Name == managedOverridesVolume {
			found = true
		}
	}
	if !found {
		t.Fatal("expected the overrides volume for Secret-only overrides")
	}
}

func TestDesiredNodeGroupStatefulSetWiresConfigOverrides(t *testing.T) {
	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		BootstrapProperties: map[string]nifiv1alpha1.ConfigOverrideValue{"java.arg.debug": "-XX:+PrintFlagsFinal"},
	})
	group := &nifiv1alpha1.NiFiNodeGroup{ObjectMeta: metav1.ObjectMeta{Name: "workers", Namespace: "default"}}
	spec := desiredNodeGroupStatefulSetSpec(cluster, group, nil, 1, "", "payload-checksum", nil)

	foundVolume := false
	for _, volume := range spec.Template.Spec.Volumes {
		if volume.Name == managedOverridesVolume {
			foundVolume = true
		}
	}
	if !foundVolume {
		t.Fatal("expected the node group pods to mount the cluster's overrides Secret")
	}
	if spec.Template.Annotations[managedOverridesChecksumAnnotation] != "payload-checksum" {
		t.Fatal("expected the overrides checksum annotation on node group pods")
	}
}

func TestReconcileManagedClusterConfigOverridesLifecycle(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))

	cluster := overridesTestCluster(&nifiv1alpha1.NiFiClusterConfigOverrides{
		NiFiProperties: map[string]nifiv1alpha1.ConfigOverrideValue{"nifi.queue.swap.threshold": "15000"},
	})
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	ctx := context.Background()

	resolved, err := reconciler.reconcileManagedClusterConfigOverrides(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.checksum == "" {
		t.Fatal("expected a payload checksum")
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: managedClusterOverridesSecretName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	if got := string(secret.Data[overridesNiFiPropertiesKey]); got != "nifi.queue.swap.threshold=15000\n" {
		t.Fatalf("unexpected payload Secret content: %q", got)
	}
	if len(secret.OwnerReferences) == 0 || secret.OwnerReferences[0].Name != cluster.Name {
		t.Fatal("expected the overrides Secret to be owned by the cluster")
	}
	// Cleanup is a no-op while overrides are still declared.
	if err := reconciler.cleanupManagedClusterConfigOverrides(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, secret); err != nil {
		t.Fatalf("payload Secret should survive cleanup while overrides exist: %v", err)
	}

	// An updated override value flows into the payload Secret.
	cluster.Spec.ConfigOverrides.NiFiProperties["nifi.queue.swap.threshold"] = "30000"
	if _, err := reconciler.reconcileManagedClusterConfigOverrides(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	if got := string(secret.Data[overridesNiFiPropertiesKey]); got != "nifi.queue.swap.threshold=30000\n" {
		t.Fatalf("expected updated payload content, got %q", got)
	}

	// Clearing the overrides deletes the payload Secret; a second cleanup stays quiet.
	cluster.Spec.ConfigOverrides = nil
	if _, err := reconciler.reconcileManagedClusterConfigOverrides(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.cleanupManagedClusterConfigOverrides(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, secret); !apierrors.IsNotFound(err) {
		t.Fatalf("expected the payload Secret to be deleted, got %v", err)
	}
	if err := reconciler.cleanupManagedClusterConfigOverrides(ctx, cluster); err != nil {
		t.Fatal(err)
	}
}
