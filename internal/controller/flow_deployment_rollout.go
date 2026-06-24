package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const flowDriftCheckInterval = time.Minute

func (r *NiFiFlowDeploymentReconciler) snapshotReader() nifi.FlowSnapshotReader {
	if r.FlowSnapshotReader != nil {
		return r.FlowSnapshotReader
	}
	if reader, ok := r.FlowSnapshotClient.(nifi.FlowSnapshotReader); ok {
		return reader
	}
	return nifi.HTTPFlowSnapshotClient{}
}

func (r *NiFiFlowDeploymentReconciler) processGroupScheduler() nifi.ProcessGroupScheduler {
	if r.ProcessGroupScheduler != nil {
		return r.ProcessGroupScheduler
	}
	return nifi.HTTPProcessGroupScheduler{}
}

func ensureActiveFlowRollout(deployment *nifiv1alpha1.NiFiFlowDeployment, version string, digest string, operation string) *nifiv1alpha1.FlowRolloutStatus {
	active := deployment.Status.ActiveRollout
	if active != nil && active.TargetDigest == digest && active.Operation == operation {
		return active
	}
	active = &nifiv1alpha1.FlowRolloutStatus{
		Phase: "Preparing", Strategy: resolvedRolloutStrategy(deployment), Operation: operation,
		TargetVersion: version, TargetDigest: digest,
		PreviousVersion: deployment.Status.DeployedVersion, PreviousDigest: deployment.Status.ArtifactDigest,
		StartedAt: metav1.Now(),
	}
	deployment.Status.ActiveRollout = active
	return active
}

func (r *NiFiFlowDeploymentReconciler) prepareFlowRollout(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, version string, digest string) (bool, error) {
	active := ensureActiveFlowRollout(deployment, version, digest, "Rollout")
	if active.Phase != "Preparing" {
		return true, nil
	}
	switch active.Strategy {
	case "StopAllThenApply":
		// Drain queues while components are still running, before stopping the group.
		drained, err := r.drainProcessGroupQueues(ctx, deployment, endpoint, deployment.Status.ProcessGroupID, active.StartedAt.Time)
		if err != nil {
			return false, fmt.Errorf("drain queues before %s rollout: %w", active.Strategy, err)
		}
		if !drained {
			if deployment.Status.SyncState != "DrainingQueues" {
				deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "DrainingQueues", "Draining queues before stopping the flow for replacement.")
				deployment.Status.SyncState = "DrainingQueues"
				return false, r.Status().Update(ctx, deployment)
			}
			return false, nil
		}
		if err := r.processGroupScheduler().ScheduleProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID, "STOPPED"); err != nil {
			return false, fmt.Errorf("stop process group before %s rollout: %w", active.Strategy, err)
		}
		active.Phase = "Prepared"
		deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "RolloutPrepared", "The flow deployment is prepared for replacement.")
		deployment.Status.SyncState = "RolloutPrepared"
		deployment.Status.Sync.LastError = ""
		if err := r.Status().Update(ctx, deployment); err != nil {
			return false, err
		}
		return false, nil
	}
	active.Phase = "Prepared"
	return true, nil
}

func (r *NiFiFlowDeploymentReconciler) finalizeFlowRolloutState(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string) error {
	strategy := resolvedRolloutStrategy(deployment)
	if deployment.Status.ActiveRollout != nil && deployment.Status.ActiveRollout.Strategy != "" {
		strategy = deployment.Status.ActiveRollout.Strategy
	}
	switch strategy {
	case "StopAllThenApply":
		return r.processGroupScheduler().ScheduleProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID, "RUNNING")
	default:
		return nil
	}
}

func (r *NiFiFlowDeploymentReconciler) markSnapshotDeploymentInSync(
	ctx context.Context,
	deployment *nifiv1alpha1.NiFiFlowDeployment,
	processGroupID string,
	revisionVersion int64,
	version string,
	digest string,
	desiredContentDigest string,
	liveContentDigest string,
	snapshot []byte,
) error {
	if deployment.Status.LastSuccessful == nil || deployment.Status.LastSuccessful.Digest != digest {
		if err := r.recordSuccessfulFlowDeployment(ctx, deployment, snapshot, version, digest, "Succeeded"); err != nil {
			return err
		}
	}
	r.trimFlowDeploymentHistory(ctx, deployment)
	deployment.Status.CommonStatus.MarkReady(deployment.Generation, "RolloutComplete", "The NiFi flow deployment rollout is complete and in sync.")
	deployment.Status.CommonStatus.Drift.Status = "InSync"
	deployment.Status.CommonStatus.Drift.Differences = nil
	deployment.Status.CommonStatus.Drift.LastDetectedTime = nil
	deployment.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionDriftDetected, metav1.ConditionFalse, "NoDrift", "The live NiFi flow matches the desired snapshot.", deployment.Generation)
	deployment.Status.NiFiID = processGroupID
	deployment.Status.ProcessGroupID = processGroupID
	deployment.Status.Revision.Version = revisionVersion
	deployment.Status.DeployedVersion = version
	deployment.Status.ArtifactDigest = digest
	deployment.Status.DesiredContentDigest = desiredContentDigest
	deployment.Status.LiveContentDigest = liveContentDigest
	deployment.Status.SyncState = "InSync"
	deployment.Status.Sync.LastError = ""
	deployment.Status.LatestReplaceRequest = nil
	deployment.Status.ActiveRollout = nil
	if deployment.Status.LastRollback != nil && deployment.Generation > deployment.Status.LastRollback.FailedGeneration {
		deployment.Status.LastRollback = nil
	}
	return r.Status().Update(ctx, deployment)
}

func (r *NiFiFlowDeploymentReconciler) markSnapshotDeploymentDrift(
	ctx context.Context,
	deployment *nifiv1alpha1.NiFiFlowDeployment,
	desiredContentDigest string,
	liveContentDigest string,
	differences []string,
	mode nifiv1alpha1.DriftPolicyMode,
) error {
	now := metav1.Now()
	message := fmt.Sprintf("Detected live flow drift in %d field(s).", len(differences))
	if mode == nifiv1alpha1.DriftPolicyFail {
		deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "DriftDetected", message)
		deployment.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionReconciling, metav1.ConditionFalse, "DriftBlocked", "Rollout is blocked by the Fail drift policy.", deployment.Generation)
	} else {
		deployment.Status.CommonStatus.MarkReady(deployment.Generation, "DriftWarning", message)
	}
	deployment.Status.CommonStatus.Drift.Status = "Drifted"
	deployment.Status.CommonStatus.Drift.Differences = differences
	deployment.Status.CommonStatus.Drift.LastDetectedTime = &now
	deployment.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionDriftDetected, metav1.ConditionTrue, "DriftDetected", message, deployment.Generation)
	deployment.Status.DesiredContentDigest = desiredContentDigest
	deployment.Status.LiveContentDigest = liveContentDigest
	deployment.Status.SyncState = "Drifted"
	if mode == nifiv1alpha1.DriftPolicyFail {
		deployment.Status.Sync.LastError = message
	} else {
		deployment.Status.Sync.LastError = ""
	}
	return r.Status().Update(ctx, deployment)
}

func (r *NiFiFlowDeploymentReconciler) markSnapshotDeploymentRolledBack(
	ctx context.Context,
	deployment *nifiv1alpha1.NiFiFlowDeployment,
	revisionVersion int64,
	history *nifiv1alpha1.FlowDeploymentHistory,
	snapshot []byte,
) error {
	failedGeneration := deployment.Generation
	failedVersion := ""
	failedDigest := ""
	if deployment.Status.ActiveRollout != nil {
		failedVersion = deployment.Status.ActiveRollout.PreviousVersion
		failedDigest = deployment.Status.ActiveRollout.PreviousDigest
	}
	if deployment.Status.LastRollback != nil {
		failedGeneration = deployment.Status.LastRollback.FailedGeneration
		failedVersion = deployment.Status.LastRollback.FailedVersion
		failedDigest = deployment.Status.LastRollback.FailedDigest
	}
	if err := r.recordSuccessfulFlowDeployment(ctx, deployment, snapshot, history.Version, history.Digest, "RolledBack"); err != nil {
		return err
	}
	r.trimFlowDeploymentHistory(ctx, deployment)
	now := metav1.Now()
	deployment.Status.LastRollback = &nifiv1alpha1.FlowRollbackStatus{
		FailedGeneration: failedGeneration, FailedVersion: failedVersion, FailedDigest: failedDigest,
		RestoredVersion: history.Version, RestoredDigest: history.Digest, CompletedAt: &now,
		Message: "The previous successful flow snapshot was restored.",
	}
	deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "RollbackComplete", "The previous successful flow was restored after rollout failure.")
	deployment.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionReconciling, metav1.ConditionFalse, "RollbackComplete", "Change the failed source or update the deployment to retry.", deployment.Generation)
	deployment.Status.Revision.Version = revisionVersion
	deployment.Status.DeployedVersion = history.Version
	deployment.Status.ArtifactDigest = history.Digest
	deployment.Status.SyncState = "RolledBack"
	deployment.Status.Sync.LastError = ""
	deployment.Status.LatestReplaceRequest = nil
	deployment.Status.ActiveRollout = nil
	return r.Status().Update(ctx, deployment)
}

func rollbackBlocksTarget(deployment *nifiv1alpha1.NiFiFlowDeployment, digest string) bool {
	rollback := deployment.Status.LastRollback
	return rollback != nil && rollback.CompletedAt != nil && rollback.FailedGeneration == deployment.Generation && rollback.FailedDigest == digest
}

func rolloutRequeue() ctrl.Result {
	return ctrl.Result{RequeueAfter: flowDriftCheckInterval}
}
