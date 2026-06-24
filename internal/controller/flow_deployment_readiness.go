package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// readinessGateEnabled reports whether a post-rollout readiness gate is configured.
func readinessGateEnabled(deployment *nifiv1alpha1.NiFiFlowDeployment) bool {
	return deployment.Spec.Rollout.Readiness != nil
}

type rolloutReadinessGate struct {
	requireValid   bool
	requireCS      bool
	maxUnavailable int32
	timeout        time.Duration
}

func resolveRolloutReadinessGate(deployment *nifiv1alpha1.NiFiFlowDeployment) rolloutReadinessGate {
	gate := rolloutReadinessGate{requireValid: true, requireCS: true, maxUnavailable: 0, timeout: 300 * time.Second}
	if rd := deployment.Spec.Rollout.Readiness; rd != nil {
		if rd.RequireValidComponents != nil {
			gate.requireValid = *rd.RequireValidComponents
		}
		if rd.RequireEnabledControllerServices != nil {
			gate.requireCS = *rd.RequireEnabledControllerServices
		}
		gate.maxUnavailable = rd.MaxUnavailable
		if rd.TimeoutSeconds > 0 {
			gate.timeout = time.Duration(rd.TimeoutSeconds) * time.Second
		}
	}
	return gate
}

// evaluateRolloutReadiness enables controller services (best-effort) and reports whether
// the deployed group's invalid component count is within the configured tolerance.
func (r *NiFiFlowDeploymentReconciler) evaluateRolloutReadiness(ctx context.Context, endpoint string, processGroupID string, gate rolloutReadinessGate) (bool, string, error) {
	if gate.requireCS {
		_ = r.blueGreenClient().EnableControllerServices(ctx, endpoint, processGroupID)
	}
	if !gate.requireValid {
		return true, "", nil
	}
	pg, err := r.processGroupClientOrDefault().GetProcessGroup(ctx, endpoint, processGroupID)
	if err != nil {
		return false, "", err
	}
	if pg == nil {
		return false, "", fmt.Errorf("deployed process group %s was not found", processGroupID)
	}
	if pg.InvalidCount > gate.maxUnavailable {
		return false, fmt.Sprintf("%d invalid component(s) exceed maxUnavailable=%d", pg.InvalidCount, gate.maxUnavailable), nil
	}
	return true, "", nil
}

// enterRolloutReadiness transitions a completed in-place replace into the readiness-wait
// phase so the rollout is only marked in sync once the deployed flow is healthy.
func (r *NiFiFlowDeploymentReconciler) enterRolloutReadiness(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, version string, digest string) (ctrl.Result, error) {
	active := ensureActiveFlowRollout(deployment, version, digest, "Rollout")
	active.Phase = bgPhaseAwaitingReadiness
	if active.ReadinessStartedAt == nil {
		now := metav1.Now()
		active.ReadinessStartedAt = &now
	}
	deployment.Status.LatestReplaceRequest = nil
	deployment.Status.SyncState = "AwaitingReadiness"
	deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "AwaitingReadiness", "Waiting for the deployed flow components to become ready.")
	return ctrl.Result{RequeueAfter: 5 * time.Second}, r.Status().Update(ctx, deployment)
}

// reconcileRolloutReadiness is the AwaitingReadiness handler for in-place rollouts.
func (r *NiFiFlowDeploymentReconciler) reconcileRolloutReadiness(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, version string, digest string, snapshot json.RawMessage) (ctrl.Result, error) {
	gate := resolveRolloutReadinessGate(deployment)
	processGroupID := deployment.Status.ProcessGroupID
	ready, reason, err := r.evaluateRolloutReadiness(ctx, endpoint, processGroupID, gate)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "ReadinessCheckFailed", fmt.Errorf("evaluate rollout readiness: %w", err))
	}
	if ready {
		return r.finalizeSuccessfulRollout(ctx, deployment, endpoint, processGroupID, version, digest, snapshot)
	}

	active := deployment.Status.ActiveRollout
	firstWait := active.ReadinessStartedAt == nil
	if firstWait {
		now := metav1.Now()
		active.ReadinessStartedAt = &now
	}
	if gate.timeout > 0 && active.ReadinessStartedAt != nil && time.Since(active.ReadinessStartedAt.Time) > gate.timeout {
		return r.handleRolloutReadinessTimeout(ctx, deployment, endpoint, reason)
	}
	if firstWait || deployment.Status.SyncState != "AwaitingReadiness" {
		deployment.Status.SyncState = "AwaitingReadiness"
		deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "AwaitingReadiness", fmt.Sprintf("Waiting for the deployed flow to become ready: %s.", reason))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.Status().Update(ctx, deployment)
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// handleRolloutReadinessTimeout fails a rollout whose deployed flow never became healthy,
// honouring the retry policy before giving up.
func (r *NiFiFlowDeploymentReconciler) handleRolloutReadinessTimeout(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, reason string) (ctrl.Result, error) {
	if retried, result, err := r.retryRolloutIfAllowed(ctx, deployment, fmt.Sprintf("readiness timed out: %s", reason)); retried {
		return result, err
	}
	active := deployment.Status.ActiveRollout
	if active != nil {
		active.ReadinessStartedAt = nil
		active.Phase = ""
	}
	deployment.Status.ActiveRollout = nil
	return r.snapshotDeploymentFailed(ctx, deployment, "ReadinessTimeout", fmt.Errorf("deployed flow did not become ready: %s", reason))
}

// finalizeSuccessfulRollout performs the drift check and marks the deployment in sync. It
// is shared by the gated readiness path and the ungated replace-completion path.
func (r *NiFiFlowDeploymentReconciler) finalizeSuccessfulRollout(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, processGroupID string, version string, digest string, targetSnapshot json.RawMessage) (ctrl.Result, error) {
	current, err := r.processGroupClientOrDefault().GetProcessGroup(ctx, endpoint, processGroupID)
	if err != nil || current == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiRefreshFailed", fmt.Errorf("refresh deployed process group: %w", err))
	}
	mode := resolvedDriftPolicy(deployment)
	desiredContentDigest := ""
	liveContentDigest := ""
	differences := []string{}
	if mode == nifiv1alpha1.DriftPolicyIgnore {
		_, desiredContentDigest, err = normalizeFlowSnapshot(targetSnapshot, deployment.Spec.DriftPolicy.IgnoreFields)
		liveContentDigest = desiredContentDigest
	} else {
		liveSnapshot, downloadErr := r.snapshotReader().DownloadProcessGroup(ctx, endpoint, processGroupID)
		if downloadErr != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "DriftCheckFailed", fmt.Errorf("download deployed NiFi flow: %w", downloadErr))
		}
		desiredContentDigest, liveContentDigest, differences, err = compareFlowSnapshots(targetSnapshot, liveSnapshot, deployment.Spec.DriftPolicy.IgnoreFields)
	}
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "DriftCheckFailed", err)
	}
	if err := r.markSnapshotDeploymentInSync(ctx, deployment, processGroupID, current.Revision.Version, version, digest, desiredContentDigest, liveContentDigest, targetSnapshot); err != nil {
		return ctrl.Result{}, err
	}
	if len(differences) > 0 && mode != nifiv1alpha1.DriftPolicyIgnore {
		if err := r.markSnapshotDeploymentDrift(ctx, deployment, desiredContentDigest, liveContentDigest, differences, mode); err != nil {
			return ctrl.Result{}, err
		}
	}
	return rolloutRequeue(), nil
}

// rolloutMaxRetries returns the configured automatic retry budget.
func rolloutMaxRetries(deployment *nifiv1alpha1.NiFiFlowDeployment) int32 {
	if deployment.Spec.Rollout.Retry != nil {
		return deployment.Spec.Rollout.Retry.MaxRetries
	}
	return 0
}

// retryRolloutIfAllowed re-attempts a failed in-place rollout when the retry budget is not
// exhausted. It returns retried=false when no retry should happen so the caller can fall
// back to rollback or failure.
func (r *NiFiFlowDeploymentReconciler) retryRolloutIfAllowed(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, reason string) (bool, ctrl.Result, error) {
	active := deployment.Status.ActiveRollout
	maxRetries := rolloutMaxRetries(deployment)
	if active == nil || active.Operation != "Rollout" || maxRetries <= 0 || active.RetryCount >= maxRetries {
		return false, ctrl.Result{}, nil
	}
	active.RetryCount++
	active.Phase = "Preparing"
	active.ReadinessStartedAt = nil
	deployment.Status.LatestReplaceRequest = nil
	deployment.Status.SyncState = "RolloutRetrying"
	deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "RolloutRetrying", fmt.Sprintf("Retrying rollout (%d/%d): %s.", active.RetryCount, maxRetries, reason))
	return true, ctrl.Result{RequeueAfter: 10 * time.Second}, r.Status().Update(ctx, deployment)
}

// reconcileRolloutCancellation aborts an in-flight rollout when spec.rollout.cancel is set.
func (r *NiFiFlowDeploymentReconciler) reconcileRolloutCancellation(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string) (ctrl.Result, error) {
	if blueGreenRolloutInProgress(deployment) {
		return r.blueGreenStartRollback(ctx, deployment, "rollout cancelled")
	}
	if pending := deployment.Status.LatestReplaceRequest; pending != nil && pending.ID != "" {
		flowSnapshots := r.flowSnapshotClientOrDefault()
		_ = flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, pending.ID)
	}
	deployment.Status.LatestReplaceRequest = nil
	deployment.Status.ActiveRollout = nil
	deployment.Status.SyncState = "RolloutCancelled"
	return r.snapshotDeploymentFailed(ctx, deployment, "RolloutCancelled", fmt.Errorf("rollout cancelled by spec.rollout.cancel"))
}

// drainProcessGroupQueues waits for the deployed group's connection queues to drain before
// a disruptive rollout step, applying the configured on-timeout behaviour. It returns
// done=false while still waiting.
func (r *NiFiFlowDeploymentReconciler) drainProcessGroupQueues(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, processGroupID string, startedAt time.Time) (bool, error) {
	policy := deployment.Spec.Rollout.QueuePolicy
	if policy == nil || !policy.Enabled {
		return true, nil
	}
	bgClient := r.blueGreenClient()
	connections, err := bgClient.ListProcessGroupConnections(ctx, endpoint, processGroupID)
	if err != nil {
		return false, fmt.Errorf("list connections to drain: %w", err)
	}
	timeout := time.Duration(policy.TimeoutSeconds) * time.Second
	if policy.TimeoutSeconds == 0 {
		timeout = 60 * time.Second
	}
	expired := time.Since(startedAt) > timeout
	pending := []nifi.ConnectionEntity{}
	for i := range connections {
		id := connections[i].ID
		if id == "" {
			id = connections[i].Component.ID
		}
		count, err := bgClient.ConnectionQueueCount(ctx, endpoint, id)
		if err != nil {
			return false, fmt.Errorf("read queue for connection %s: %w", id, err)
		}
		if count > 0 {
			pending = append(pending, connections[i])
		}
	}
	if len(pending) == 0 {
		return true, nil
	}
	if !expired {
		return false, nil
	}
	switch policy.OnTimeout {
	case "Drop":
		for i := range pending {
			id := pending[i].ID
			if id == "" {
				id = pending[i].Component.ID
			}
			if err := bgClient.DropConnectionQueue(ctx, endpoint, id); err != nil {
				return false, fmt.Errorf("drop queue for connection %s: %w", id, err)
			}
		}
		return true, nil
	case "Proceed":
		return true, nil
	default: // Fail
		return false, fmt.Errorf("%d queue(s) did not drain within %s", len(pending), timeout)
	}
}
