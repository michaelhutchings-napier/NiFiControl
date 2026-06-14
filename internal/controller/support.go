package controller

import (
	"context"
	"fmt"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func markClusterReachable(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiCluster) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "ClusterReachable", "The NiFi API endpoint is reachable.")
	obj.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionClusterReachable, metav1.ConditionTrue, "ClusterReachable", "The NiFi API endpoint is reachable.", obj.Generation)
	if obj.Spec.API != nil {
		obj.Status.Endpoint = obj.Spec.API.URI
	}
	return c.Status().Update(ctx, obj)
}

func markClusterUnreachable(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiCluster, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, "ClusterUnreachable", message)
	obj.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionClusterReachable, metav1.ConditionFalse, "ClusterUnreachable", message, obj.Generation)
	if obj.Spec.API != nil {
		obj.Status.Endpoint = obj.Spec.API.URI
	}
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

func markParameterContextReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiParameterContext, nifiID string, revisionVersion int64) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "ParameterContextReady", "The NiFi parameter context is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markParameterContextNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiParameterContext, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
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
	cluster, waitingFor, err := clusterDependency(ctx, c, namespace, ref)
	if err != nil || cluster == nil {
		return waitingFor, err
	}
	return nil, nil
}

func readyClusterForReference(ctx context.Context, c client.Client, namespace string, ref nifiv1alpha1.ClusterReference) (*nifiv1alpha1.NiFiCluster, []string, error) {
	return clusterDependency(ctx, c, namespace, ref)
}

func clusterDependency(ctx context.Context, c client.Client, namespace string, ref nifiv1alpha1.ClusterReference) (*nifiv1alpha1.NiFiCluster, []string, error) {
	if ref.Name == "" {
		return nil, []string{"clusterRef.name"}, nil
	}

	refNamespace := clusterRefNamespace(namespace, ref)

	cluster := &nifiv1alpha1.NiFiCluster{}
	key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
	if err := c.Get(ctx, key, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, []string{fmt.Sprintf("NiFiCluster/%s/%s", refNamespace, ref.Name)}, nil
		}
		return nil, nil, err
	}
	if !cluster.Status.Ready {
		return nil, []string{fmt.Sprintf("NiFiCluster/%s/%s:Ready", refNamespace, ref.Name)}, nil
	}

	return cluster, nil, nil
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
