package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// repositoryVolumeBinding ties a dedicated repository volume claim to the NiFi data
// directory it backs. The claim name doubles as the pod volume name: the StatefulSet
// controller injects a volume named after each claim template into the pods it creates.
type repositoryVolumeBinding struct {
	claimName string
	directory string
	volume    *nifiv1alpha1.NiFiClusterRepositoryVolumeSpec
}

// repositoryVolumeBindings lists the configured dedicated repository volumes in a stable
// order. Repositories without an entry stay on the main data volume.
func repositoryVolumeBindings(storage nifiv1alpha1.NiFiClusterStorageSpec) []repositoryVolumeBinding {
	repositories := storage.Repositories
	if repositories == nil {
		return nil
	}
	bindings := []repositoryVolumeBinding{}
	if repositories.FlowFile != nil {
		bindings = append(bindings, repositoryVolumeBinding{"flowfile-repository", "flowfile_repository", repositories.FlowFile})
	}
	if repositories.Content != nil {
		bindings = append(bindings, repositoryVolumeBinding{"content-repository", "content_repository", repositories.Content})
	}
	if repositories.Provenance != nil {
		bindings = append(bindings, repositoryVolumeBinding{"provenance-repository", "provenance_repository", repositories.Provenance})
	}
	if repositories.Database != nil {
		bindings = append(bindings, repositoryVolumeBinding{"database-repository", "database_repository", repositories.Database})
	}
	return bindings
}

// nodeVolumeClaims builds the claim templates for a node pool: the main data volume plus
// one claim per dedicated repository volume. A repository volume inherits the main
// volume's storage class and access modes unless it sets its own.
func nodeVolumeClaims(storage nifiv1alpha1.NiFiClusterStorageSpec, labels, annotations map[string]string) []corev1.PersistentVolumeClaim {
	claims := []corev1.PersistentVolumeClaim{nodeVolumeClaim(storage, labels, annotations)}
	for _, binding := range repositoryVolumeBindings(storage) {
		repositoryStorage := nifiv1alpha1.NiFiClusterStorageSpec{
			Size:             binding.volume.Size,
			StorageClassName: binding.volume.StorageClassName,
			AccessModes:      binding.volume.AccessModes,
		}
		if repositoryStorage.StorageClassName == nil {
			repositoryStorage.StorageClassName = storage.StorageClassName
		}
		if len(repositoryStorage.AccessModes) == 0 {
			repositoryStorage.AccessModes = storage.AccessModes
		}
		claim := nodeVolumeClaim(repositoryStorage, labels, annotations)
		claim.Name = binding.claimName
		claims = append(claims, claim)
	}
	return claims
}

// recreateStatefulSetOnClaimChange orphan-deletes a StatefulSet whose claim templates no
// longer match the desired set, so the caller's CreateOrUpdate recreates it around the
// running pods. VolumeClaimTemplates are immutable; after the recreate, the rolling
// update replaces each pod, and the StatefulSet controller binds the pod's new claims as
// it is created. Repository data is not migrated between volumes.
func (r *NiFiClusterReconciler) recreateStatefulSetOnClaimChange(ctx context.Context, name, namespace string, desired []corev1.PersistentVolumeClaim) error {
	return recreateOnClaimChange(ctx, r.Client, name, namespace, desired)
}

func recreateOnClaimChange(ctx context.Context, c client.Client, name, namespace string, desired []corev1.PersistentVolumeClaim) error {
	existing := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if claimTemplateNamesMatch(existing.Spec.VolumeClaimTemplates, desired) {
		return nil
	}
	if err := c.Delete(ctx, existing, client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	// Wait briefly for the object to disappear so the immediate re-create does not race
	// the deletion; if it lingers, the next reconcile completes the recreate.
	for i := 0; i < 10; i++ {
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &appsv1.StatefulSet{}); apierrors.IsNotFound(err) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("StatefulSet %s/%s is being recreated for a volume claim change; requeueing", namespace, name)
}

// claimTemplateNamesMatch reports whether an existing StatefulSet already has claim
// templates with the desired names. VolumeClaimTemplates are immutable, so a mismatch
// means the StatefulSet must be recreated (orphaning its pods) rather than updated.
func claimTemplateNamesMatch(existing, desired []corev1.PersistentVolumeClaim) bool {
	if len(existing) != len(desired) {
		return false
	}
	names := make(map[string]bool, len(existing))
	for _, claim := range existing {
		names[claim.Name] = true
	}
	for _, claim := range desired {
		if !names[claim.Name] {
			return false
		}
	}
	return true
}
