package controller

import (
	"context"
	"fmt"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const NiFiControlFinalizer = "nifi.controlnifi.io/finalizer"

const clusterRefIndexField = "spec.clusterRef"

func ensureFinalizer(ctx context.Context, c client.Client, obj client.Object) (bool, error) {
	if controllerutil.ContainsFinalizer(obj, NiFiControlFinalizer) {
		return false, nil
	}
	controllerutil.AddFinalizer(obj, NiFiControlFinalizer)
	return true, c.Update(ctx, obj)
}

func removeFinalizer(ctx context.Context, c client.Client, obj client.Object) (bool, error) {
	if !controllerutil.ContainsFinalizer(obj, NiFiControlFinalizer) {
		return false, nil
	}
	controllerutil.RemoveFinalizer(obj, NiFiControlFinalizer)
	return true, c.Update(ctx, obj)
}

func markClusterAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiCluster) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markRegistryClientAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiRegistryClient) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markRegistryClientWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiRegistryClient, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markParameterContextAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiParameterContext) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markParameterContextWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiParameterContext, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markControllerServiceAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiControllerService) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markControllerServiceWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiControllerService, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markFlowBundleAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowBundle) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func clusterDependencyWaitingFor(ctx context.Context, c client.Client, namespace string, ref nifiv1alpha1.ClusterReference) ([]string, error) {
	if ref.Name == "" {
		return []string{"clusterRef.name"}, nil
	}

	refNamespace := clusterRefNamespace(namespace, ref)

	cluster := &nifiv1alpha1.NiFiCluster{}
	key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
	if err := c.Get(ctx, key, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return []string{fmt.Sprintf("NiFiCluster/%s/%s", refNamespace, ref.Name)}, nil
		}
		return nil, err
	}
	if !cluster.Status.Ready {
		return []string{fmt.Sprintf("NiFiCluster/%s/%s:Ready", refNamespace, ref.Name)}, nil
	}

	return nil, nil
}

func clusterRefIndexValue(namespace string, ref nifiv1alpha1.ClusterReference) string {
	if ref.Name == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s", clusterRefNamespace(namespace, ref), ref.Name)
}

func clusterRefNamespace(namespace string, ref nifiv1alpha1.ClusterReference) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return namespace
}

func waitingForChanged(current []string, desired []string) bool {
	if len(current) != len(desired) {
		return true
	}
	for i := range current {
		if current[i] != desired[i] {
			return true
		}
	}
	return false
}
