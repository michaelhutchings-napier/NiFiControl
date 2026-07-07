package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func storageTestCluster(repositories *nifiv1alpha1.NiFiClusterRepositoryStorageSpec) *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "edge", Namespace: "default"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode: nifiv1alpha1.ClusterModeInternal,
			Storage: nifiv1alpha1.NiFiClusterStorageSpec{
				Enabled:          ptr.To(true),
				Size:             resource.MustParse("10Gi"),
				StorageClassName: ptr.To("fast"),
				Repositories:     repositories,
			},
		},
	}
}

func TestDedicatedRepositoryVolumesProduceClaimsAndMounts(t *testing.T) {
	cluster := storageTestCluster(&nifiv1alpha1.NiFiClusterRepositoryStorageSpec{
		Content:    &nifiv1alpha1.NiFiClusterRepositoryVolumeSpec{Size: resource.MustParse("100Gi"), StorageClassName: ptr.To("bulk")},
		Provenance: &nifiv1alpha1.NiFiClusterRepositoryVolumeSpec{Size: resource.MustParse("50Gi")},
	})
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)

	claims := map[string]corev1.PersistentVolumeClaim{}
	for _, claim := range spec.VolumeClaimTemplates {
		claims[claim.Name] = claim
	}
	if len(claims) != 3 {
		t.Fatalf("expected data + 2 repository claims, got %v", len(claims))
	}
	content, ok := claims["content-repository"]
	if !ok {
		t.Fatal("expected a content-repository claim template")
	}
	if content.Spec.Resources.Requests.Storage().String() != "100Gi" || *content.Spec.StorageClassName != "bulk" {
		t.Fatalf("content claim not sized/classed as requested: %+v", content.Spec)
	}
	provenance := claims["provenance-repository"]
	// Unset storage class inherits the main data volume's class.
	if provenance.Spec.StorageClassName == nil || *provenance.Spec.StorageClassName != "fast" {
		t.Fatalf("provenance claim should inherit the data storage class, got %+v", provenance.Spec.StorageClassName)
	}

	mounts := map[string]corev1.VolumeMount{}
	for _, mount := range spec.Template.Spec.Containers[0].VolumeMounts {
		mounts[mount.MountPath] = mount
	}
	contentMount := mounts["/opt/nifi/nifi-current/content_repository"]
	if contentMount.Name != "content-repository" || contentMount.SubPath != "" {
		t.Fatalf("content_repository should mount its dedicated claim without a subPath: %+v", contentMount)
	}
	flowfileMount := mounts["/opt/nifi/nifi-current/flowfile_repository"]
	if flowfileMount.Name != managedDataVolume || flowfileMount.SubPath != "flowfile_repository" {
		t.Fatalf("flowfile_repository should stay on the data volume: %+v", flowfileMount)
	}
}

func TestNoRepositoriesKeepsSingleDataClaim(t *testing.T) {
	spec := desiredManagedClusterStatefulSetSpec(storageTestCluster(nil), nil, "", nil)
	if len(spec.VolumeClaimTemplates) != 1 || spec.VolumeClaimTemplates[0].Name != managedDataVolume {
		t.Fatalf("expected only the data claim, got %+v", spec.VolumeClaimTemplates)
	}
}

func TestRecreateOnClaimChangeOrphanDeletesStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))

	existing := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "edge-nifi", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: managedDataVolume}}},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()
	ctx := context.Background()

	// Same claim names: no-op.
	same := []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: managedDataVolume}}}
	if err := recreateOnClaimChange(ctx, k8sClient, "edge-nifi", "default", same); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "edge-nifi", Namespace: "default"}, &appsv1.StatefulSet{}); err != nil {
		t.Fatalf("StatefulSet should survive when claims match: %v", err)
	}

	// New repository claim: the StatefulSet is deleted so the caller can recreate it.
	changed := append(same, corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "content-repository"}})
	if err := recreateOnClaimChange(ctx, k8sClient, "edge-nifi", "default", changed); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "edge-nifi", Namespace: "default"}, &appsv1.StatefulSet{}); err == nil {
		t.Fatal("expected the StatefulSet to be deleted for the claim change")
	}

	// Missing StatefulSet: no-op.
	if err := recreateOnClaimChange(ctx, k8sClient, "edge-nifi", "default", changed); err != nil {
		t.Fatal(err)
	}
}
