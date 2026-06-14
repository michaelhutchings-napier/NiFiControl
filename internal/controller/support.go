package controller

import (
	"context"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const NiFiControlFinalizer = "nifi.controlnifi.io/finalizer"

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

func markParameterContextAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiParameterContext) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markControllerServiceAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiControllerService) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
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
