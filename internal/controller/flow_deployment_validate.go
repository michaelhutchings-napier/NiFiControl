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

const (
	flowValidationPhaseValidating = "Validating"
	flowValidationPhaseCleanup    = "Cleanup"

	// flowValidationSettleTimeout bounds how long to wait for imported components to finish
	// validating (e.g. while controller services finish enabling) before reporting a result.
	flowValidationSettleTimeout = 90 * time.Second
	flowValidationPollInterval  = 3 * time.Second
	flowValidationCleanupPoll   = 3 * time.Second

	// Caps keep the status object bounded regardless of how broken a flow is.
	maxReportedInvalidComponents = 25
	maxReportedComponentErrors   = 5
)

func (r *NiFiFlowDeploymentReconciler) flowValidationClientOrDefault() nifi.FlowValidationClient {
	if r.FlowValidationClient != nil {
		return r.FlowValidationClient
	}
	return nifi.HTTPFlowValidationClient{}
}

// reconcileFlowValidation runs a spec.validateOnly dry run as a small state machine: import a
// temporary detached process group, wait for its components to settle, record the result, and
// delete the temporary group. Nothing is wired into the live flow.
func (r *NiFiFlowDeploymentReconciler) reconcileFlowValidation(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment, endpoint string, parentID string, snapshot json.RawMessage, version string, digest string) (ctrl.Result, error) {
	// Refuse to validate a deployment that already created a live process group; the validate-only
	// path never touches it, so proceeding would orphan it. Validate with a separate resource.
	if instance.Status.ProcessGroupID != "" {
		message := "validateOnly cannot be enabled on a deployment that already created a live process group; validate with a separate NiFiFlowDeployment."
		if shouldMarkFlowDeploymentNotReady(instance, "ValidateOnlyConflict", message) {
			return ctrl.Result{}, markFlowDeploymentNotReady(ctx, r.Client, instance, "ValidateOnlyConflict", message)
		}
		return ctrl.Result{}, nil
	}

	switch instance.Status.ValidationPhase {
	case flowValidationPhaseValidating:
		return r.reconcileFlowValidationInspect(ctx, instance, endpoint, version, digest)
	case flowValidationPhaseCleanup:
		return r.reconcileFlowValidationCleanup(ctx, instance, endpoint)
	default:
		return r.startFlowValidation(ctx, instance, endpoint, parentID, snapshot, version, digest)
	}
}

func (r *NiFiFlowDeploymentReconciler) startFlowValidation(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment, endpoint string, parentID string, snapshot json.RawMessage, version string, digest string) (ctrl.Result, error) {
	// Churn guard: the current content was already validated at this generation. Do not re-import.
	if result := instance.Status.ValidationResult; result != nil &&
		result.CheckedDigest == digest && instance.Status.ObservedGeneration == instance.Generation {
		return ctrl.Result{}, nil
	}

	named, err := flowSnapshotWithName(snapshot, flowValidationGroupName(instance))
	if err != nil {
		return r.flowValidationOperationalError(ctx, instance, "ValidationPrepareFailed", fmt.Sprintf("prepare validation snapshot: %v", err))
	}
	// Idempotency: a prior attempt whose status did not persist (or whose import errored after
	// NiFi created the group) can leave a temporary group of our deterministic name behind. Remove
	// any such leftovers before importing so a failing import cannot accumulate groups.
	r.cleanupStaleValidationGroups(ctx, endpoint, parentID, flowValidationGroupName(instance))

	imported, err := r.flowSnapshotClientOrDefault().ImportProcessGroup(ctx, endpoint, parentID, named)
	if err != nil {
		return r.flowValidationOperationalError(ctx, instance, "ValidationImportFailed", fmt.Sprintf("import flow for validation: %v", err))
	}
	pgID := ""
	if imported != nil {
		pgID = processGroupEntityID(*imported)
	}
	if pgID == "" {
		return r.flowValidationOperationalError(ctx, instance, "ValidationImportFailed", "NiFi did not return an imported process group id")
	}

	// Enable controller services so components that reference them validate as they would on a real
	// deploy (a referenced-but-disabled service otherwise reports the referencing component invalid).
	_ = r.flowValidationClientOrDefault().SetControllerServicesState(ctx, endpoint, pgID, nifi.RunStateEnabled)

	now := metav1.Now()
	instance.Status.ValidationProcessGroupID = pgID
	instance.Status.ValidationPhase = flowValidationPhaseValidating
	instance.Status.ValidationStartedAt = &now
	instance.Status.SyncState = "Validating"
	instance.Status.Dependencies.Ready = true
	instance.Status.Dependencies.WaitingFor = nil
	instance.Status.CommonStatus.MarkNotReady(instance.Generation, "Validating", "Validating the flow in a temporary process group.")
	return ctrl.Result{RequeueAfter: flowValidationPollInterval}, r.Status().Update(ctx, instance)
}

// cleanupStaleValidationGroups removes any temporary validation process group of the given name
// directly under the parent. It is best-effort: leftovers from an interrupted run are recognisable
// by their deterministic name, and clearing them keeps a repeatedly-failing import from piling up
// orphaned groups on the canvas.
func (r *NiFiFlowDeploymentReconciler) cleanupStaleValidationGroups(ctx context.Context, endpoint string, parentID string, name string) {
	children, err := r.flowValidationClientOrDefault().ListChildProcessGroups(ctx, endpoint, parentID)
	if err != nil {
		return
	}
	for i := range children {
		if children[i].Component.Name != name {
			continue
		}
		id := processGroupEntityID(children[i])
		if id != "" {
			_, _ = r.tryDeleteValidationProcessGroup(ctx, endpoint, id)
		}
	}
}

func (r *NiFiFlowDeploymentReconciler) reconcileFlowValidationInspect(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment, endpoint string, version string, digest string) (ctrl.Result, error) {
	pgID := instance.Status.ValidationProcessGroupID
	if pgID == "" {
		// Lost track of the temporary group; restart the run.
		instance.Status.ValidationPhase = ""
		return ctrl.Result{Requeue: true}, r.Status().Update(ctx, instance)
	}

	report, err := r.flowValidationClientOrDefault().InspectValidation(ctx, endpoint, pgID)
	if err != nil {
		return r.flowValidationOperationalError(ctx, instance, "ValidationInspectFailed", fmt.Sprintf("inspect validation: %v", err))
	}

	settleExpired := instance.Status.ValidationStartedAt != nil &&
		time.Since(instance.Status.ValidationStartedAt.Time) > flowValidationSettleTimeout
	if report.ValidatingCount > 0 && !settleExpired {
		return ctrl.Result{RequeueAfter: flowValidationPollInterval}, nil
	}

	// Components have settled (or the settle timeout expired): record the result and move to cleanup.
	instance.Status.ValidationResult = buildFlowValidationResult(report, version, digest, settleExpired)
	instance.Status.ValidationPhase = flowValidationPhaseCleanup
	instance.Status.SyncState = "ValidationCleanup"
	// Disable controller services first so the temporary group can be deleted.
	_ = r.flowValidationClientOrDefault().SetControllerServicesState(ctx, endpoint, pgID, nifi.RunStateDisabled)
	return ctrl.Result{RequeueAfter: flowValidationCleanupPoll}, r.Status().Update(ctx, instance)
}

func (r *NiFiFlowDeploymentReconciler) reconcileFlowValidationCleanup(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment, endpoint string) (ctrl.Result, error) {
	pgID := instance.Status.ValidationProcessGroupID
	if pgID != "" {
		gone, err := r.tryDeleteValidationProcessGroup(ctx, endpoint, pgID)
		if err != nil {
			// Transient (e.g. services still disabling); keep retrying without churning status.
			return ctrl.Result{RequeueAfter: flowValidationCleanupPoll}, nil
		}
		if !gone {
			return ctrl.Result{RequeueAfter: flowValidationCleanupPoll}, nil
		}
	}
	instance.Status.ValidationProcessGroupID = ""
	instance.Status.ValidationPhase = ""
	instance.Status.ValidationStartedAt = nil
	return r.finalizeFlowValidation(ctx, instance)
}

// finalizeFlowValidation sets the terminal Ready condition from the recorded result. It requeues
// nothing: the churn guard in startFlowValidation prevents re-validating the same content.
func (r *NiFiFlowDeploymentReconciler) finalizeFlowValidation(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment) (ctrl.Result, error) {
	result := instance.Status.ValidationResult
	instance.Status.SyncState = "ValidationComplete"
	instance.Status.Dependencies.Ready = true
	instance.Status.Dependencies.WaitingFor = nil
	if result != nil && result.Valid {
		instance.Status.CommonStatus.MarkReady(instance.Generation, "ValidationSucceeded", result.Message)
		instance.Status.Sync.LastError = ""
	} else {
		message := "flow validation failed"
		if result != nil && result.Message != "" {
			message = result.Message
		}
		instance.Status.CommonStatus.MarkNotReady(instance.Generation, "ValidationFailed", message)
		instance.Status.Sync.LastError = message
	}
	return ctrl.Result{}, r.Status().Update(ctx, instance)
}

// flowValidationOperationalError reports a NiFi/API failure (not an invalid flow) and requeues so
// the validation retries. It keeps ValidationPhase intact so an in-progress run resumes.
func (r *NiFiFlowDeploymentReconciler) flowValidationOperationalError(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment, reason string, message string) (ctrl.Result, error) {
	if shouldMarkFlowDeploymentNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowDeploymentNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// tryDeleteValidationProcessGroup stops the group, disables its controller services, and deletes it.
// It returns gone=true once the group no longer exists.
func (r *NiFiFlowDeploymentReconciler) tryDeleteValidationProcessGroup(ctx context.Context, endpoint string, pgID string) (bool, error) {
	_ = r.processGroupScheduler().ScheduleProcessGroup(ctx, endpoint, pgID, nifi.RunStateStopped)
	_ = r.flowValidationClientOrDefault().SetControllerServicesState(ctx, endpoint, pgID, nifi.RunStateDisabled)
	processGroups := r.processGroupClientOrDefault()
	existing, err := processGroups.GetProcessGroup(ctx, endpoint, pgID)
	if err != nil {
		if nifi.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if existing == nil {
		return true, nil
	}
	if err := processGroups.DeleteProcessGroup(ctx, endpoint, pgID, existing.Revision.Version); err != nil {
		if nifi.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// deleteValidationProcessGroup best-effort removes a temporary validation group during CR deletion.
func (r *NiFiFlowDeploymentReconciler) deleteValidationProcessGroup(ctx context.Context, endpoint string, pgID string) {
	_, _ = r.tryDeleteValidationProcessGroup(ctx, endpoint, pgID)
}

func flowValidationGroupName(instance *nifiv1alpha1.NiFiFlowDeployment) string {
	return fmt.Sprintf("nificontrol-validate-%s", instance.Name)
}

// flowSnapshotWithName overrides the imported group's name so a leaked temporary group is
// recognisable on the canvas. The resolved snapshot always carries a flowContents object.
func flowSnapshotWithName(snapshot json.RawMessage, name string) (json.RawMessage, error) {
	var decoded map[string]any
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		return nil, err
	}
	if flowContents, ok := decoded["flowContents"].(map[string]any); ok {
		flowContents["name"] = name
	}
	return json.Marshal(decoded)
}

func buildFlowValidationResult(report nifi.FlowValidationReport, version string, digest string, settleTimedOut bool) *nifiv1alpha1.FlowValidationResult {
	now := metav1.Now()
	invalidCount := int32(len(report.Invalid))
	valid := invalidCount == 0
	inconclusive := settleTimedOut && report.ValidatingCount > 0
	if inconclusive {
		valid = false
	}

	components := make([]nifiv1alpha1.FlowInvalidComponent, 0, len(report.Invalid))
	truncated := false
	for i := range report.Invalid {
		if i >= maxReportedInvalidComponents {
			truncated = true
			break
		}
		c := report.Invalid[i]
		errs := c.ValidationErrors
		if len(errs) > maxReportedComponentErrors {
			errs = errs[:maxReportedComponentErrors]
		}
		components = append(components, nifiv1alpha1.FlowInvalidComponent{
			Kind: c.Kind, ID: c.ID, Name: c.Name, Type: c.Type,
			ProcessGroupID: c.ProcessGroupID, Errors: errs,
		})
	}

	var message string
	switch {
	case valid:
		message = fmt.Sprintf("Flow is valid (%d component(s) checked).", report.Total)
	default:
		message = fmt.Sprintf("Flow validation failed: %d invalid component(s) of %d checked.", invalidCount, report.Total)
	}
	if truncated {
		message += fmt.Sprintf(" Reporting the first %d.", maxReportedInvalidComponents)
	}
	if inconclusive {
		message += fmt.Sprintf(" %d component(s) had not finished validating within %s.", report.ValidatingCount, flowValidationSettleTimeout)
	}

	return &nifiv1alpha1.FlowValidationResult{
		Valid:             valid,
		CheckedVersion:    version,
		CheckedDigest:     digest,
		InvalidCount:      invalidCount,
		InvalidComponents: components,
		CheckedAt:         now,
		Message:           message,
	}
}
