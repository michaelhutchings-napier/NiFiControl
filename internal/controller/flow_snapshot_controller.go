package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func canonicalFlowSnapshot(snapshot *runtime.RawExtension, targetName string) (json.RawMessage, string, error) {
	if snapshot == nil {
		return nil, "", nil
	}
	raw := snapshot.Raw
	if len(raw) == 0 && snapshot.Object != nil {
		marshaled, err := json.Marshal(snapshot.Object)
		if err != nil {
			return nil, "", err
		}
		raw = marshaled
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, "", fmt.Errorf("snapshot is empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, "", fmt.Errorf("snapshot is not valid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, "", fmt.Errorf("snapshot must contain exactly one JSON value")
	}
	if wrapped, ok := decoded["versionedFlowSnapshot"].(map[string]any); ok {
		decoded = wrapped
	}
	flowContents, ok := decoded["flowContents"].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("snapshot must contain an object field named flowContents")
	}
	canonicalSource, err := json.Marshal(decoded)
	if err != nil {
		return nil, "", err
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(canonicalSource))
	if targetName != "" {
		flowContents["name"] = targetName
	}
	canonicalTarget, err := json.Marshal(decoded)
	if err != nil {
		return nil, "", err
	}
	return canonicalTarget, digest, nil
}

func resolvedFlowDeploymentSnapshot(ctx context.Context, c client.Client, deployment *nifiv1alpha1.NiFiFlowDeployment) (json.RawMessage, string, string, error) {
	version := deployment.Spec.Source.Version
	var source *nifiv1alpha1.FlowBundleSource
	artifactDigest := ""
	if deployment.Spec.Source.BundleRef != nil {
		ref := *deployment.Spec.Source.BundleRef
		bundle := &nifiv1alpha1.NiFiFlowBundle{}
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(deployment.Namespace, ref)}
		if err := c.Get(ctx, key, bundle); err != nil {
			return nil, "", "", err
		}
		source = &bundle.Spec.Source
		artifactDigest = bundle.Status.ArtifactDigest
		if version == "" {
			version = bundle.Status.ResolvedRevision
		}
		if version == "" {
			version = bundle.Spec.Version
		}
	} else {
		source = deployment.Spec.Source.Inline
	}
	if source == nil || source.Snapshot == nil {
		return nil, "", "", nil
	}
	snapshot, digest, err := canonicalFlowSnapshot(source.Snapshot, deployment.Spec.Target.ProcessGroupName)
	if err != nil {
		return nil, "", "", err
	}
	if artifactDigest == "" {
		artifactDigest = digest
	}
	if version == "" {
		version = artifactDigest
	}
	return snapshot, version, artifactDigest, nil
}

func (r *NiFiFlowDeploymentReconciler) reconcileSnapshotFlowDeployment(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, parentID string, snapshot json.RawMessage, version string, digest string) (ctrl.Result, error) {
	flowSnapshots := r.FlowSnapshotClient
	if flowSnapshots == nil {
		flowSnapshots = nifi.HTTPFlowSnapshotClient{}
	}
	processGroups := r.ProcessGroupClient
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}

	if pending := deployment.Status.LatestReplaceRequest; pending != nil && pending.ID != "" {
		if pending.FailureReason != "" {
			return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceFailed", fmt.Errorf("%s", pending.FailureReason))
		}
		if pending.Complete {
			return r.completeFlowReplace(ctx, deployment, endpoint, flowSnapshots, processGroups, pending)
		}
		return r.reconcilePendingFlowReplace(ctx, deployment, endpoint, flowSnapshots, processGroups)
	}

	if deployment.Status.ProcessGroupID == "" {
		imported, err := flowSnapshots.ImportProcessGroup(ctx, endpoint, parentID, snapshot)
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotImportFailed", fmt.Errorf("failed to import NiFi flow snapshot: %w", err))
		}
		if imported == nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotImportFailed", fmt.Errorf("NiFi returned an empty process group import response"))
		}
		processGroupID := processGroupEntityID(*imported)
		if processGroupID == "" {
			return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotImportFailed", fmt.Errorf("NiFi did not return an imported process group ID"))
		}
		if err := markFlowDeploymentSnapshotImported(ctx, r.Client, deployment, processGroupID, imported.Revision.Version, version, digest); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	existing, err := processGroups.GetProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiGetFailed", fmt.Errorf("failed to get imported process group: %w", err))
	}
	if existing == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiGetFailed", fmt.Errorf("NiFi returned an empty imported process group response"))
	}

	if deployment.Status.ArtifactDigest != digest || deployment.Status.DeployedVersion != version {
		request, err := flowSnapshots.CreateProcessGroupReplaceRequest(ctx, endpoint, deployment.Status.ProcessGroupID, existing.Revision.Version, snapshot)
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceCreateFailed", fmt.Errorf("failed to create NiFi flow replace request: %w", err))
		}
		status := flowReplaceRequestStatus(request, digest, version)
		if status.ID == "" {
			return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceCreateFailed", fmt.Errorf("NiFi did not return a flow replace request ID"))
		}
		deployment.Status.LatestReplaceRequest = status
		if status.FailureReason != "" {
			if err := flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, status.ID); err == nil {
				status.ID = ""
			}
			return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceFailed", fmt.Errorf("%s", status.FailureReason))
		}
		if status.Complete {
			if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	current, err := r.reconcileSnapshotDeploymentMetadata(ctx, deployment, endpoint, processGroups, existing)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotMetadataFailed", err)
	}
	if !flowDeploymentStatusMatches(deployment, deployment.Status.ProcessGroupID, current.Revision.Version, version, digest, "SnapshotInSync") {
		return ctrl.Result{}, markFlowDeploymentReady(ctx, r.Client, deployment, deployment.Status.ProcessGroupID, current.Revision.Version, version, digest, "SnapshotInSync")
	}
	return ctrl.Result{}, nil
}

func (r *NiFiFlowDeploymentReconciler) reconcilePendingFlowReplace(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, flowSnapshots nifi.FlowSnapshotClient, processGroups nifi.ProcessGroupClient) (ctrl.Result, error) {
	pending := deployment.Status.LatestReplaceRequest
	request, err := flowSnapshots.GetProcessGroupReplaceRequest(ctx, endpoint, pending.ID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceGetFailed", fmt.Errorf("failed to get NiFi flow replace request: %w", err))
	}
	status := flowReplaceRequestStatus(request, pending.TargetDigest, pending.TargetVersion)
	if status.ID == "" {
		status.ID = pending.ID
	}
	deployment.Status.LatestReplaceRequest = status
	if status.FailureReason != "" {
		if err := flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, status.ID); err == nil {
			status.ID = ""
		}
		return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceFailed", fmt.Errorf("%s", status.FailureReason))
	}
	if !status.Complete {
		if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *NiFiFlowDeploymentReconciler) completeFlowReplace(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, flowSnapshots nifi.FlowSnapshotClient, processGroups nifi.ProcessGroupClient, status *nifiv1alpha1.FlowReplaceRequestStatus) (ctrl.Result, error) {
	existing, err := processGroups.GetProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiRefreshFailed", fmt.Errorf("failed to refresh replaced process group: %w", err))
	}
	if existing == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiRefreshFailed", fmt.Errorf("NiFi returned an empty replaced process group response"))
	}
	current, err := r.reconcileSnapshotDeploymentMetadata(ctx, deployment, endpoint, processGroups, existing)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotMetadataFailed", err)
	}
	if err := flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, status.ID); err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceCleanupFailed", fmt.Errorf("failed to clean up NiFi flow replace request: %w", err))
	}
	status.Complete = true
	status.ID = ""
	deployment.Status.LatestReplaceRequest = status
	return ctrl.Result{}, markFlowDeploymentReady(ctx, r.Client, deployment, deployment.Status.ProcessGroupID, current.Revision.Version, status.TargetVersion, status.TargetDigest, "SnapshotInSync")
}

func (r *NiFiFlowDeploymentReconciler) reconcileSnapshotDeploymentMetadata(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, processGroups nifi.ProcessGroupClient, existing *nifi.ProcessGroupEntity) (*nifi.ProcessGroupEntity, error) {
	update := *existing
	needsUpdate := false
	if deployment.Spec.Target.ProcessGroupName != "" && update.Component.Name != deployment.Spec.Target.ProcessGroupName {
		update.Component.Name = deployment.Spec.Target.ProcessGroupName
		needsUpdate = true
	}
	if deployment.Spec.ParameterContextRef != nil {
		ref := *deployment.Spec.ParameterContextRef
		parameterContext := &nifiv1alpha1.NiFiParameterContext{}
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(deployment.Namespace, ref)}
		if err := r.Get(ctx, key, parameterContext); err != nil {
			return nil, err
		}
		if componentReferenceID(update.Component.ParameterContext) != parameterContext.Status.NiFiID {
			update.Component.ParameterContext = &nifi.ComponentReference{ID: parameterContext.Status.NiFiID}
			needsUpdate = true
		}
	}
	if !needsUpdate {
		return existing, nil
	}
	update.ID = processGroupEntityID(*existing)
	update.Component.ID = update.ID
	updated, err := processGroups.UpdateProcessGroup(ctx, endpoint, update)
	if err != nil {
		return nil, fmt.Errorf("failed to update imported process group metadata: %w", err)
	}
	if updated == nil {
		return existing, nil
	}
	return updated, nil
}

func (r *NiFiFlowDeploymentReconciler) snapshotDeploymentFailed(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, reason string, failure error) (ctrl.Result, error) {
	message := failure.Error()
	if shouldMarkFlowDeploymentNotReady(deployment, reason, message) {
		if err := markFlowDeploymentNotReady(ctx, r.Client, deployment, reason, message); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func flowReplaceRequestStatus(entity *nifi.ProcessGroupReplaceRequestEntity, targetDigest string, targetVersion string) *nifiv1alpha1.FlowReplaceRequestStatus {
	status := &nifiv1alpha1.FlowReplaceRequestStatus{TargetDigest: targetDigest, TargetVersion: targetVersion}
	if entity == nil {
		return status
	}
	status.ID = entity.Request.RequestID
	status.State = entity.Request.State
	status.Complete = entity.Request.Complete
	status.FailureReason = entity.Request.FailureReason
	status.PercentCompleted = entity.Request.PercentCompleted
	return status
}
