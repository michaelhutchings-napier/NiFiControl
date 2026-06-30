package controller

import (
	"context"
	"fmt"
	"strings"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const NiFiControlFinalizer = "nifi.controlnifi.io/finalizer"

const clusterRefIndexField = "spec.clusterRef"

// recordEvent emits a Kubernetes Event when a recorder is configured. It is nil-safe so
// reconcilers constructed without a recorder (notably in unit tests) are unaffected.
func recordEvent(recorder record.EventRecorder, obj runtime.Object, eventType, reason, message string) {
	if recorder == nil || obj == nil {
		return
	}
	recorder.Event(obj, eventType, reason, message)
}

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

func markRegistryClientReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiRegistryClient, nifiID string, revisionVersion int64, resolvedType string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "RegistryClientReady", "The NiFi registry client is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ResolvedType = resolvedType
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markRegistryClientNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiRegistryClient, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
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

func markParameterContextUpdateRunning(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiParameterContext, request *nifiv1alpha1.ParameterContextUpdateRequestStatus) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, "UpdateRunning", "The NiFi parameter context update request is still running.")
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.LatestUpdateRequest = request
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

func markControllerServiceReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiControllerService, nifiID string, revisionVersion int64, validationStatus string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "ControllerServiceReady", "The NiFi controller service is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ValidationStatus = validationStatus
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markControllerServiceNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiControllerService, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markUserAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUser) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markUserWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUser, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markUserGroupAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUserGroup) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markUserGroupWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUserGroup, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markProcessGroupAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessGroup) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markProcessGroupReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessGroup, nifiID string, revisionVersion int64, parentProcessGroupID string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "ProcessGroupReady", "The NiFi process group is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ParentProcessGroupID = parentProcessGroupID
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markProcessGroupNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessGroup, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markProcessGroupWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessGroup, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
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

func markFlowBundleReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowBundle, artifactDigest string, resolvedRevision string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "FlowBundleReady", "The NiFi flow bundle source is resolved.")
	obj.Status.ArtifactDigest = artifactDigest
	obj.Status.ResolvedRevision = resolvedRevision
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markFlowBundleNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowBundle, reason string, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markFlowBundleWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowBundle, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment, processGroupID string, revisionVersion int64, deployedVersion string, artifactDigest string, syncState string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "FlowDeploymentReady", "The NiFi flow deployment target is reconciled.")
	obj.Status.NiFiID = processGroupID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ProcessGroupID = processGroupID
	obj.Status.DeployedVersion = deployedVersion
	obj.Status.ArtifactDigest = artifactDigest
	obj.Status.SyncState = syncState
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentSnapshotImported(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment, processGroupID string, revisionVersion int64, deployedVersion string, artifactDigest string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, "SnapshotMetadataPending", "The full flow snapshot is imported; target metadata is pending reconciliation.")
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.NiFiID = processGroupID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ProcessGroupID = processGroupID
	obj.Status.DeployedVersion = deployedVersion
	obj.Status.ArtifactDigest = artifactDigest
	obj.Status.SyncState = "MetadataPending"
	obj.Status.LatestReplaceRequest = nil
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentReplaceRunning(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment, request *nifiv1alpha1.FlowReplaceRequestStatus) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, "FlowReplaceRunning", "NiFi is replacing the deployed flow contents.")
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.LatestReplaceRequest = request
	obj.Status.SyncState = "Replacing"
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markFlowDeploymentWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFlowDeployment, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markProcessorAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessor) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markProcessorReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessor, nifiID string, revisionVersion int64, parentProcessGroupID string, validationStatus string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "ProcessorReady", "The NiFi processor is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ParentProcessGroupID = parentProcessGroupID
	obj.Status.ValidationStatus = validationStatus
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markProcessorNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessor, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markProcessorWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiProcessor, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markInputPortAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiInputPort) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markInputPortReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiInputPort, nifiID string, revisionVersion int64, parentProcessGroupID string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "InputPortReady", "The NiFi input port is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ParentProcessGroupID = parentProcessGroupID
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markInputPortNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiInputPort, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markInputPortWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiInputPort, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markOutputPortAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiOutputPort) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markOutputPortReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiOutputPort, nifiID string, revisionVersion int64, parentProcessGroupID string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "OutputPortReady", "The NiFi output port is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ParentProcessGroupID = parentProcessGroupID
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markOutputPortNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiOutputPort, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markOutputPortWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiOutputPort, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markConnectionAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiConnection) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markConnectionReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiConnection, nifiID string, revisionVersion int64, sourceID string, destinationID string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "ConnectionReady", "The NiFi connection is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.SourceID = sourceID
	obj.Status.DestinationID = destinationID
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markConnectionNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiConnection, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markConnectionWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiConnection, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markReportingTaskAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiReportingTask) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markReportingTaskWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiReportingTask, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markFunnelAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFunnel) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markFunnelReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFunnel, nifiID string, revisionVersion int64, parentProcessGroupID string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "FunnelReady", "The NiFi funnel is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ParentProcessGroupID = parentProcessGroupID
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markFunnelNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFunnel, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markFunnelWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiFunnel, waitingFor []string) error {
	obj.Status.CommonStatus.MarkWaitingForDependencies(obj.Generation, waitingFor)
	return c.Status().Update(ctx, obj)
}

func markLabelAccepted(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiLabel) error {
	obj.Status.CommonStatus.MarkAccepted(obj.Generation)
	return c.Status().Update(ctx, obj)
}

func markLabelReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiLabel, nifiID string, revisionVersion int64, parentProcessGroupID string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "LabelReady", "The NiFi label is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.ParentProcessGroupID = parentProcessGroupID
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markLabelNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiLabel, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func markLabelWaitingForDependencies(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiLabel, waitingFor []string) error {
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
	if err := configureClusterHTTPClient(ctx, c, cluster); err != nil {
		return nil, nil, err
	}

	return cluster, nil, nil
}

func configureClusterHTTPClient(ctx context.Context, c client.Client, cluster *nifiv1alpha1.NiFiCluster) error {
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" && resolvedClusterMode(cluster) == nifiv1alpha1.ClusterModeInternal {
		endpoint = managedClusterEndpoint(cluster)
	}
	if endpoint == "" {
		return nil
	}
	config := nifi.HTTPClientConfig{BaseURI: endpoint}

	// Operator-managed internal TLS: present the operator client certificate for mutual
	// TLS and trust the cluster CA. Never disable verification for a managed cluster.
	if internalTLSEnabled(cluster) {
		if cluster.Status.TLS == nil || !cluster.Status.TLS.Ready || cluster.Status.TLS.ClientSecretName == "" {
			return nil
		}
		material, err := tlsClientMaterial(ctx, c, cluster.Namespace, cluster.Status.TLS.ClientSecretName)
		if err != nil {
			return fmt.Errorf("resolve operator client certificate: %w", err)
		}
		config.ClientCertData = material.cert
		config.ClientKeyData = material.key
		config.CAData = material.ca
		if cluster.Spec.API != nil && cluster.Spec.API.Timeout != nil {
			config.Timeout = cluster.Spec.API.Timeout.Duration
		}
		httpClient, err := nifi.NewHTTPClient(config)
		if err != nil {
			return err
		}
		return nifi.RegisterHTTPClient(endpoint, httpClient)
	}

	if api := cluster.Spec.API; api != nil {
		if api.Timeout != nil {
			config.Timeout = api.Timeout.Duration
		}
		if api.TLS != nil {
			config.ServerName = api.TLS.ServerName
			config.InsecureSkipVerify = api.TLS.InsecureSkipVerify
			if api.TLS.CASecretKeyRef != nil {
				value, err := secretKeyValue(ctx, c, cluster.Namespace, api.TLS.CASecretKeyRef)
				if err != nil {
					return fmt.Errorf("resolve NiFi API CA: %w", err)
				}
				config.CAData = value
			}
		}
		if api.Auth != nil {
			var err error
			switch {
			case api.Auth.ClientCertificate != nil:
				config.ClientCertData, config.ClientKeyData, err = clientCertificateMaterial(ctx, c, cluster.Namespace, api.Auth.ClientCertificate)
			case api.Auth.BearerTokenSecretKeyRef != nil:
				value, valueErr := secretKeyValue(ctx, c, cluster.Namespace, api.Auth.BearerTokenSecretKeyRef)
				err = valueErr
				config.BearerToken = strings.TrimSpace(string(value))
			default:
				username, usernameErr := secretKeyValue(ctx, c, cluster.Namespace, api.Auth.UsernameSecretKeyRef)
				password, passwordErr := secretKeyValue(ctx, c, cluster.Namespace, api.Auth.PasswordSecretKeyRef)
				if usernameErr != nil {
					err = usernameErr
				} else {
					err = passwordErr
				}
				config.Username = string(username)
				config.Password = string(password)
			}
			if err != nil {
				return fmt.Errorf("resolve NiFi API credentials: %w", err)
			}
		}
	}
	httpClient, err := nifi.NewHTTPClient(config)
	if err != nil {
		return err
	}
	return nifi.RegisterHTTPClient(endpoint, httpClient)
}

type tlsClientMaterials struct {
	cert []byte
	key  []byte
	ca   []byte
}

// tlsClientMaterial loads the PEM client certificate and key from a TLS Secret for the
// operator's mTLS REST client. ca.crt is optional; when present it pins trust, otherwise
// the client uses the system trust store.
func tlsClientMaterial(ctx context.Context, c client.Client, namespace, secretName string) (tlsClientMaterials, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return tlsClientMaterials{}, err
	}
	cert := secret.Data[tlsCertKey]
	key := secret.Data[tlsKeyKey]
	if len(cert) == 0 || len(key) == 0 {
		return tlsClientMaterials{}, fmt.Errorf("Secret %s/%s is missing %s or %s", namespace, secretName, tlsCertKey, tlsKeyKey)
	}
	return tlsClientMaterials{cert: cert, key: key, ca: secret.Data[tlsCAKey]}, nil
}

// clientCertificateMaterial loads PEM client certificate and key from a Secret referenced
// by an external cluster's mTLS auth configuration.
func clientCertificateMaterial(ctx context.Context, c client.Client, namespace string, ref *nifiv1alpha1.NiFiAPIClientCertificate) ([]byte, []byte, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: ref.SecretName, Namespace: namespace}, secret); err != nil {
		return nil, nil, err
	}
	certKey := ref.CertKey
	if certKey == "" {
		certKey = tlsCertKey
	}
	keyKey := ref.KeyKey
	if keyKey == "" {
		keyKey = tlsKeyKey
	}
	cert := secret.Data[certKey]
	key := secret.Data[keyKey]
	if len(cert) == 0 || len(key) == 0 {
		return nil, nil, fmt.Errorf("Secret %s/%s is missing %s or %s", namespace, ref.SecretName, certKey, keyKey)
	}
	return cert, key, nil
}

func secretKeyValue(ctx context.Context, c client.Client, namespace string, ref *nifiv1alpha1.SecretKeyRef) ([]byte, error) {
	if ref == nil {
		return nil, fmt.Errorf("secret key reference is not configured")
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: ref.Name, Namespace: namespace}
	if err := c.Get(ctx, key, secret); err != nil {
		return nil, err
	}
	value, ok := secret.Data[ref.Key]
	if !ok {
		return nil, fmt.Errorf("Secret %s does not contain key %q", key.String(), ref.Key)
	}
	return value, nil
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
