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

// BlueGreen rollout phases stored in FlowRolloutStatus.Phase.
const (
	bgPhaseDeployingCandidate = "DeployingCandidate"
	bgPhaseAwaitingReadiness  = "AwaitingReadiness"
	bgPhaseSwitchingTraffic   = "SwitchingTraffic"
	bgPhasePromoting          = "Promoting"
	bgPhaseRollingBackTraffic = "RollingBackTraffic"

	bgDirectionInbound  = "Inbound"
	bgDirectionOutbound = "Outbound"
)

func (r *NiFiFlowDeploymentReconciler) blueGreenClient() nifi.BlueGreenClient {
	if r.BlueGreenClient != nil {
		return r.BlueGreenClient
	}
	return nifi.HTTPBlueGreenClient{}
}

func (r *NiFiFlowDeploymentReconciler) processGroupClientOrDefault() nifi.ProcessGroupClient {
	if r.ProcessGroupClient != nil {
		return r.ProcessGroupClient
	}
	return nifi.HTTPProcessGroupClient{}
}

func (r *NiFiFlowDeploymentReconciler) flowSnapshotClientOrDefault() nifi.FlowSnapshotClient {
	if r.FlowSnapshotClient != nil {
		return r.FlowSnapshotClient
	}
	return nifi.HTTPFlowSnapshotClient{}
}

func blueGreenStrategy(deployment *nifiv1alpha1.NiFiFlowDeployment) nifiv1alpha1.BlueGreenStrategy {
	resolved := nifiv1alpha1.BlueGreenStrategy{DrainTimeoutSeconds: 120, OnDrainTimeout: "Fail", ReadinessTimeoutSeconds: 300}
	enabled := true
	resolved.RequireEnabledControllerServices = &enabled
	if spec := deployment.Spec.Rollout.BlueGreen; spec != nil {
		if spec.DrainTimeoutSeconds != 0 {
			resolved.DrainTimeoutSeconds = spec.DrainTimeoutSeconds
		}
		if spec.OnDrainTimeout != "" {
			resolved.OnDrainTimeout = spec.OnDrainTimeout
		}
		if spec.ReadinessTimeoutSeconds != 0 {
			resolved.ReadinessTimeoutSeconds = spec.ReadinessTimeoutSeconds
		}
		if spec.RequireEnabledControllerServices != nil {
			resolved.RequireEnabledControllerServices = spec.RequireEnabledControllerServices
		}
	}
	return resolved
}

func blueGreenRolloutInProgress(deployment *nifiv1alpha1.NiFiFlowDeployment) bool {
	active := deployment.Status.ActiveRollout
	return active != nil && active.Operation == "BlueGreen" && active.BlueGreen != nil
}

func blueGreenCandidateName(deployment *nifiv1alpha1.NiFiFlowDeployment) string {
	name := deployment.Spec.Target.ProcessGroupName
	if name == "" {
		name = deployment.Name
	}
	return name + "-candidate"
}

// reconcileBlueGreenRollout drives the transactional BlueGreen state machine. It advances
// at most one durable step per reconcile, persisting status so an interruption resumes or
// rolls back cleanly.
func (r *NiFiFlowDeploymentReconciler) reconcileBlueGreenRollout(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, parentID string, snapshot json.RawMessage, version string, digest string) (ctrl.Result, error) {
	active := ensureActiveFlowRollout(deployment, version, digest, "BlueGreen")
	active.Strategy = "BlueGreen"
	if active.BlueGreen == nil {
		active.Phase = bgPhaseDeployingCandidate
		active.BlueGreen = &nifiv1alpha1.BlueGreenRolloutStatus{
			BlueProcessGroupID:   deployment.Status.ProcessGroupID,
			ParentProcessGroupID: parentID,
		}
		deployment.Status.SyncState = "BlueGreenRollout"
		deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "BlueGreenRollout", "Starting transactional BlueGreen rollout.")
		return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
	}

	strategy := blueGreenStrategy(deployment)
	switch active.Phase {
	case bgPhaseDeployingCandidate:
		return r.blueGreenDeployCandidate(ctx, deployment, endpoint, parentID, snapshot)
	case bgPhaseAwaitingReadiness:
		return r.blueGreenAwaitReadiness(ctx, deployment, endpoint, strategy)
	case bgPhaseSwitchingTraffic:
		return r.blueGreenSwitchTraffic(ctx, deployment, endpoint, strategy)
	case bgPhasePromoting:
		return r.blueGreenPromote(ctx, deployment, endpoint, version, digest, snapshot)
	case bgPhaseRollingBackTraffic:
		return r.blueGreenRollbackTraffic(ctx, deployment, endpoint)
	default:
		active.Phase = bgPhaseDeployingCandidate
		return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
	}
}

func (r *NiFiFlowDeploymentReconciler) blueGreenDeployCandidate(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, parentID string, snapshot json.RawMessage) (ctrl.Result, error) {
	active := deployment.Status.ActiveRollout
	if active.BlueGreen.CandidateProcessGroupID == "" {
		candidateSnapshot, err := renameFlowSnapshot(snapshot, blueGreenCandidateName(deployment))
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenCandidateImportFailed", fmt.Errorf("prepare candidate snapshot: %w", err))
		}
		imported, err := r.flowSnapshotClientOrDefault().ImportProcessGroup(ctx, endpoint, parentID, candidateSnapshot)
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenCandidateImportFailed", fmt.Errorf("import BlueGreen candidate: %w", err))
		}
		if imported == nil || processGroupEntityID(*imported) == "" {
			return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenCandidateImportFailed", fmt.Errorf("NiFi returned no candidate process group"))
		}
		active.BlueGreen.CandidateProcessGroupID = processGroupEntityID(*imported)
	}
	active.Phase = bgPhaseAwaitingReadiness
	return ctrl.Result{RequeueAfter: 2 * time.Second}, r.Status().Update(ctx, deployment)
}

func (r *NiFiFlowDeploymentReconciler) blueGreenAwaitReadiness(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, strategy nifiv1alpha1.BlueGreenStrategy) (ctrl.Result, error) {
	active := deployment.Status.ActiveRollout
	candidateID := active.BlueGreen.CandidateProcessGroupID
	bgClient := r.blueGreenClient()

	if strategy.RequireEnabledControllerServices != nil && *strategy.RequireEnabledControllerServices {
		// Best-effort: a candidate with no controller services makes NiFi's bulk-enable
		// endpoint return an error, and a transient enable failure should not block the
		// rollout. Component validity (checked below) is the real readiness gate; if
		// services genuinely fail to enable, dependent components stay invalid and the
		// readiness timeout reports it.
		_ = bgClient.EnableControllerServices(ctx, endpoint, candidateID)
	}

	candidate, err := r.processGroupClientOrDefault().GetProcessGroup(ctx, endpoint, candidateID)
	if err != nil || candidate == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenReadinessFailed", fmt.Errorf("inspect candidate process group: %w", err))
	}
	// The candidate's own boundary ports are necessarily invalid until traffic is switched
	// to them (a process-group input/output port is invalid without an external
	// connection). Tolerate up to that many invalid components so a genuinely broken
	// internal component (an invalid processor or controller service) is still caught.
	tolerance, err := r.blueGreenBoundaryPortCount(ctx, endpoint, candidateID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenReadinessFailed", fmt.Errorf("inspect candidate ports: %w", err))
	}
	if candidate.InvalidCount > tolerance {
		if r.blueGreenReadinessExpired(active, strategy) {
			return r.blueGreenAbort(ctx, deployment, endpoint, "BlueGreenReadinessFailed", fmt.Sprintf("candidate has %d invalid component(s) beyond its %d boundary port(s) after the readiness timeout", candidate.InvalidCount, tolerance))
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Internal components are valid; the boundary ports become valid and are started once
	// traffic is switched (see blueGreenPromote).
	active.Phase = bgPhaseSwitchingTraffic
	return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
}

// blueGreenBoundaryPortCount returns the number of top-level input and output ports on the
// candidate, which are the ports that receive switched boundary connections and so may be
// invalid until the switch completes.
func (r *NiFiFlowDeploymentReconciler) blueGreenBoundaryPortCount(ctx context.Context, endpoint string, candidateID string) (int32, error) {
	bgClient := r.blueGreenClient()
	inputs, err := bgClient.ListProcessGroupInputPorts(ctx, endpoint, candidateID)
	if err != nil {
		return 0, err
	}
	outputs, err := bgClient.ListProcessGroupOutputPorts(ctx, endpoint, candidateID)
	if err != nil {
		return 0, err
	}
	return int32(len(inputs) + len(outputs)), nil
}

func (r *NiFiFlowDeploymentReconciler) blueGreenReadinessExpired(active *nifiv1alpha1.FlowRolloutStatus, strategy nifiv1alpha1.BlueGreenStrategy) bool {
	if strategy.ReadinessTimeoutSeconds <= 0 {
		return true
	}
	return time.Since(active.StartedAt.Time) > time.Duration(strategy.ReadinessTimeoutSeconds)*time.Second
}

func (r *NiFiFlowDeploymentReconciler) blueGreenSwitchTraffic(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, strategy nifiv1alpha1.BlueGreenStrategy) (ctrl.Result, error) {
	active := deployment.Status.ActiveRollout
	data := active.BlueGreen

	if !data.Inventoried {
		records, err := r.blueGreenInventory(ctx, endpoint, data.BlueProcessGroupID, data.ParentProcessGroupID)
		if err != nil {
			return r.blueGreenAbort(ctx, deployment, endpoint, "BlueGreenInventoryFailed", err.Error())
		}
		data.ExternalConnections = records
		data.Inventoried = true
		return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
	}

	if len(data.ExternalConnections) == 0 {
		active.Phase = bgPhasePromoting
		return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
	}

	greenInputs, greenOutputs, err := r.blueGreenPortMaps(ctx, endpoint, data.CandidateProcessGroupID)
	if err != nil {
		return r.blueGreenAbort(ctx, deployment, endpoint, "BlueGreenCandidatePortsFailed", err.Error())
	}

	for i := range data.ExternalConnections {
		record := &data.ExternalConnections[i]
		if record.Switched {
			continue
		}
		greenPortID := greenInputs[record.PortName]
		if record.Direction == bgDirectionOutbound {
			greenPortID = greenOutputs[record.PortName]
		}
		if greenPortID == "" {
			return r.blueGreenStartRollback(ctx, deployment, fmt.Sprintf("candidate has no %s port named %q", record.Direction, record.PortName))
		}
		done, err := r.blueGreenSwitchConnection(ctx, endpoint, record, greenPortID, data.CandidateProcessGroupID, data.ParentProcessGroupID, strategy)
		if err != nil {
			return r.blueGreenStartRollback(ctx, deployment, err.Error())
		}
		if !done {
			// Draining; persist progress and try again shortly.
			return ctrl.Result{RequeueAfter: 5 * time.Second}, r.Status().Update(ctx, deployment)
		}
		// One connection switched per reconcile so each step is durable.
		return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
	}

	active.Phase = bgPhasePromoting
	return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
}

// blueGreenSwitchConnection performs the transactional switch of one boundary connection.
// It returns done=false while waiting for the queue to drain.
func (r *NiFiFlowDeploymentReconciler) blueGreenSwitchConnection(ctx context.Context, endpoint string, record *nifiv1alpha1.ExternalConnectionRecord, greenPortID string, candidateID string, parentID string, strategy nifiv1alpha1.BlueGreenStrategy) (bool, error) {
	bgClient := r.blueGreenClient()
	component, err := decodeConnectionComponent(record.Definition)
	if err != nil {
		return false, fmt.Errorf("decode recorded connection: %w", err)
	}

	// Stop the producing side so no new flow files enter the boundary queue. For an
	// inbound connection this is the external source; for an outbound connection it is
	// the deployment's (blue) output port.
	if err := bgClient.SetComponentRunStatus(ctx, endpoint, component.Source.Type, component.Source.ID, nifi.RunStateStopped); err != nil {
		return false, fmt.Errorf("stop source %s/%s: %w", component.Source.Type, component.Source.ID, err)
	}

	if record.DrainStartedAt == nil {
		now := metav1.Now()
		record.DrainStartedAt = &now
	}
	queued, err := bgClient.ConnectionQueueCount(ctx, endpoint, record.OriginalID)
	if err != nil {
		return false, fmt.Errorf("read boundary queue: %w", err)
	}
	if queued > 0 {
		expired := strategy.DrainTimeoutSeconds <= 0 || time.Since(record.DrainStartedAt.Time) > time.Duration(strategy.DrainTimeoutSeconds)*time.Second
		if !expired {
			return false, nil
		}
		if strategy.OnDrainTimeout != "Drop" {
			return false, fmt.Errorf("boundary connection %q did not drain within %ds", record.PortName, strategy.DrainTimeoutSeconds)
		}
		if err := bgClient.DropConnectionQueue(ctx, endpoint, record.OriginalID); err != nil {
			return false, fmt.Errorf("drop boundary queue: %w", err)
		}
	}

	existing, err := bgClient.GetConnection(ctx, endpoint, record.OriginalID)
	if err != nil || existing == nil {
		return false, fmt.Errorf("get boundary connection: %w", err)
	}
	if err := bgClient.DeleteConnection(ctx, endpoint, record.OriginalID, existing.Revision.Version); err != nil {
		return false, fmt.Errorf("delete boundary connection: %w", err)
	}

	switched := component
	switched.ID = ""
	switched.ParentGroupID = parentID
	if record.Direction == bgDirectionInbound {
		switched.Destination = nifi.Connectable{ID: greenPortID, Type: nifi.ConnectableInputPort, GroupID: candidateID}
	} else {
		switched.Source = nifi.Connectable{ID: greenPortID, Type: nifi.ConnectableOutputPort, GroupID: candidateID}
	}
	created, err := bgClient.CreateConnection(ctx, endpoint, parentID, nifi.ConnectionEntity{Revision: nifi.Revision{Version: 0}, Component: switched})
	if err != nil {
		return false, fmt.Errorf("create green connection: %w", err)
	}
	record.GreenConnectionID = bgConnectionEntityID(created)
	record.Switched = true
	return true, nil
}

func (r *NiFiFlowDeploymentReconciler) blueGreenPromote(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, version string, digest string, snapshot json.RawMessage) (ctrl.Result, error) {
	active := deployment.Status.ActiveRollout
	data := active.BlueGreen
	processGroups := r.processGroupClientOrDefault()
	bgClient := r.blueGreenClient()

	candidate, err := processGroups.GetProcessGroup(ctx, endpoint, data.CandidateProcessGroupID)
	if err != nil || candidate == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenPromoteFailed", fmt.Errorf("get candidate for promotion: %w", err))
	}

	// Start the candidate now that its boundary ports have switched connections and are
	// valid; ports that were invalid during readiness could not start earlier.
	if err := r.processGroupScheduler().ScheduleProcessGroup(ctx, endpoint, data.CandidateProcessGroupID, nifi.RunStateRunning); err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenPromoteFailed", fmt.Errorf("start promoted process group: %w", err))
	}

	// Adopt the candidate as the live deployment and rename it to the target name.
	deployment.Status.ProcessGroupID = data.CandidateProcessGroupID
	current, err := r.reconcileSnapshotDeploymentMetadata(ctx, deployment, endpoint, processGroups, candidate)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenPromoteFailed", fmt.Errorf("rename promoted process group: %w", err))
	}

	// Restart external sources that were stopped while switching inbound connections.
	for i := range data.ExternalConnections {
		record := data.ExternalConnections[i]
		if record.Direction != bgDirectionInbound {
			continue
		}
		if component, err := decodeConnectionComponent(record.Definition); err == nil {
			_ = bgClient.SetComponentRunStatus(ctx, endpoint, component.Source.Type, component.Source.ID, nifi.RunStateRunning)
		}
	}

	deployment.Status.RetiringProcessGroupID = data.BlueProcessGroupID

	desiredContentDigest := ""
	if _, normalized, normErr := normalizeFlowSnapshot(snapshot, deployment.Spec.DriftPolicy.IgnoreFields); normErr == nil {
		desiredContentDigest = normalized
	}
	return rolloutRequeue(), r.markSnapshotDeploymentInSync(ctx, deployment, data.CandidateProcessGroupID, current.Revision.Version, version, digest, desiredContentDigest, desiredContentDigest, snapshot)
}

func (r *NiFiFlowDeploymentReconciler) blueGreenStartRollback(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, reason string) (ctrl.Result, error) {
	active := deployment.Status.ActiveRollout
	active.Phase = bgPhaseRollingBackTraffic
	deployment.Status.Sync.LastError = reason
	deployment.Status.CommonStatus.MarkNotReady(deployment.Generation, "BlueGreenRollingBack", fmt.Sprintf("Switching traffic back to blue: %s", reason))
	return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
}

func (r *NiFiFlowDeploymentReconciler) blueGreenRollbackTraffic(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string) (ctrl.Result, error) {
	active := deployment.Status.ActiveRollout
	data := active.BlueGreen
	bgClient := r.blueGreenClient()
	parentID := data.ParentProcessGroupID

	for i := range data.ExternalConnections {
		record := &data.ExternalConnections[i]
		if !record.Switched {
			continue
		}
		component, err := decodeConnectionComponent(record.Definition)
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenRollbackFailed", fmt.Errorf("decode recorded connection during rollback: %w", err))
		}
		if record.GreenConnectionID != "" {
			if green, err := bgClient.GetConnection(ctx, endpoint, record.GreenConnectionID); err == nil && green != nil {
				if count, _ := bgClient.ConnectionQueueCount(ctx, endpoint, record.GreenConnectionID); count > 0 {
					_ = bgClient.DropConnectionQueue(ctx, endpoint, record.GreenConnectionID)
				}
				if err := bgClient.DeleteConnection(ctx, endpoint, record.GreenConnectionID, green.Revision.Version); err != nil {
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}
		}
		restore := component
		restore.ID = ""
		restore.ParentGroupID = parentID
		created, err := bgClient.CreateConnection(ctx, endpoint, parentID, nifi.ConnectionEntity{Revision: nifi.Revision{Version: 0}, Component: restore})
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenRollbackFailed", fmt.Errorf("restore blue connection %q: %w", record.PortName, err))
		}
		record.OriginalID = bgConnectionEntityID(created)
		record.GreenConnectionID = ""
		record.Switched = false
		record.DrainStartedAt = nil
		return ctrl.Result{RequeueAfter: time.Second}, r.Status().Update(ctx, deployment)
	}

	// Restart the sources that were stopped, restoring blue to running.
	for i := range data.ExternalConnections {
		if component, err := decodeConnectionComponent(data.ExternalConnections[i].Definition); err == nil {
			_ = bgClient.SetComponentRunStatus(ctx, endpoint, component.Source.Type, component.Source.ID, nifi.RunStateRunning)
		}
	}
	if data.BlueProcessGroupID != "" {
		_ = r.processGroupScheduler().ScheduleProcessGroup(ctx, endpoint, data.BlueProcessGroupID, nifi.RunStateRunning)
	}

	r.blueGreenDeleteProcessGroup(ctx, endpoint, data.CandidateProcessGroupID)

	failure := deployment.Status.Sync.LastError
	if failure == "" {
		failure = "BlueGreen rollout failed; traffic was switched back to blue"
	}
	now := metav1.Now()
	deployment.Status.LastRollback = &nifiv1alpha1.FlowRollbackStatus{
		FailedGeneration: deployment.Generation,
		FailedVersion:    active.TargetVersion,
		FailedDigest:     active.TargetDigest,
		RestoredVersion:  deployment.Status.DeployedVersion,
		RestoredDigest:   deployment.Status.ArtifactDigest,
		CompletedAt:      &now,
		Message:          failure,
	}
	deployment.Status.ActiveRollout = nil
	r.recordFailedFlowDeployment(deployment, active.TargetVersion, active.TargetDigest, "", failure)
	r.trimFlowDeploymentHistory(ctx, deployment)
	return r.snapshotDeploymentFailed(ctx, deployment, "BlueGreenRolledBack", fmt.Errorf("%s", failure))
}

// blueGreenAbort deletes the candidate before any traffic was switched and fails the rollout.
func (r *NiFiFlowDeploymentReconciler) blueGreenAbort(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, reason string, message string) (ctrl.Result, error) {
	if data := deployment.Status.ActiveRollout.BlueGreen; data != nil {
		r.blueGreenDeleteProcessGroup(ctx, endpoint, data.CandidateProcessGroupID)
	}
	deployment.Status.ActiveRollout = nil
	return r.snapshotDeploymentFailed(ctx, deployment, reason, fmt.Errorf("%s", message))
}

func (r *NiFiFlowDeploymentReconciler) blueGreenDeleteProcessGroup(ctx context.Context, endpoint string, processGroupID string) {
	if processGroupID == "" {
		return
	}
	_ = r.processGroupScheduler().ScheduleProcessGroup(ctx, endpoint, processGroupID, nifi.RunStateStopped)
	processGroups := r.processGroupClientOrDefault()
	if existing, err := processGroups.GetProcessGroup(ctx, endpoint, processGroupID); err == nil && existing != nil {
		_ = processGroups.DeleteProcessGroup(ctx, endpoint, processGroupID, existing.Revision.Version)
	}
}

// retireBlueProcessGroup deletes a process group promoted away one reconcile earlier.
func (r *NiFiFlowDeploymentReconciler) retireBlueProcessGroup(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string) error {
	if deployment.Status.RetiringProcessGroupID == "" {
		return nil
	}
	r.blueGreenDeleteProcessGroup(ctx, endpoint, deployment.Status.RetiringProcessGroupID)
	deployment.Status.RetiringProcessGroupID = ""
	return r.Status().Update(ctx, deployment)
}

// blueGreenInventory records every boundary connection that crosses the blue deployment's
// process-group edge, matched to a blue port by name.
func (r *NiFiFlowDeploymentReconciler) blueGreenInventory(ctx context.Context, endpoint string, blueID string, parentID string) ([]nifiv1alpha1.ExternalConnectionRecord, error) {
	bgClient := r.blueGreenClient()
	connections, err := bgClient.ListProcessGroupConnections(ctx, endpoint, parentID)
	if err != nil {
		return nil, fmt.Errorf("list parent connections: %w", err)
	}
	inputs, err := bgClient.ListProcessGroupInputPorts(ctx, endpoint, blueID)
	if err != nil {
		return nil, fmt.Errorf("list blue input ports: %w", err)
	}
	outputs, err := bgClient.ListProcessGroupOutputPorts(ctx, endpoint, blueID)
	if err != nil {
		return nil, fmt.Errorf("list blue output ports: %w", err)
	}
	inputNames := portIDToName(inputs)
	outputNames := portIDToName(outputs)

	records := []nifiv1alpha1.ExternalConnectionRecord{}
	for i := range connections {
		component := connections[i].Component
		definition, marshalErr := json.Marshal(component)
		if marshalErr != nil {
			return nil, marshalErr
		}
		switch {
		case component.Destination.GroupID == blueID && component.Destination.Type == nifi.ConnectableInputPort:
			portName := inputNames[component.Destination.ID]
			if portName == "" {
				continue
			}
			if !blueGreenSwitchableSource(component.Source.Type) {
				return nil, fmt.Errorf("inbound boundary connection feeds port %q from unsupported %s source; BlueGreen cannot stop it safely", portName, component.Source.Type)
			}
			records = append(records, nifiv1alpha1.ExternalConnectionRecord{
				Direction: bgDirectionInbound, PortName: portName,
				OriginalID: bgConnectionEntityID(&connections[i]), Definition: string(definition),
			})
		case component.Source.GroupID == blueID && component.Source.Type == nifi.ConnectableOutputPort:
			portName := outputNames[component.Source.ID]
			if portName == "" {
				continue
			}
			records = append(records, nifiv1alpha1.ExternalConnectionRecord{
				Direction: bgDirectionOutbound, PortName: portName,
				OriginalID: bgConnectionEntityID(&connections[i]), Definition: string(definition),
			})
		}
	}
	return records, nil
}

func (r *NiFiFlowDeploymentReconciler) blueGreenPortMaps(ctx context.Context, endpoint string, candidateID string) (map[string]string, map[string]string, error) {
	bgClient := r.blueGreenClient()
	inputs, err := bgClient.ListProcessGroupInputPorts(ctx, endpoint, candidateID)
	if err != nil {
		return nil, nil, fmt.Errorf("list candidate input ports: %w", err)
	}
	outputs, err := bgClient.ListProcessGroupOutputPorts(ctx, endpoint, candidateID)
	if err != nil {
		return nil, nil, fmt.Errorf("list candidate output ports: %w", err)
	}
	return portNameToID(inputs), portNameToID(outputs), nil
}

func blueGreenSwitchableSource(componentType string) bool {
	switch componentType {
	case nifi.ConnectableProcessor, nifi.ConnectableInputPort, nifi.ConnectableOutputPort:
		return true
	default:
		return false
	}
}

func portIDToName(ports []nifi.PortEntity) map[string]string {
	result := make(map[string]string, len(ports))
	for _, port := range ports {
		id := port.ID
		if id == "" {
			id = port.Component.ID
		}
		name := port.Component.Name
		if id != "" && name != "" {
			result[id] = name
		}
	}
	return result
}

func portNameToID(ports []nifi.PortEntity) map[string]string {
	result := make(map[string]string, len(ports))
	for _, port := range ports {
		id := port.ID
		if id == "" {
			id = port.Component.ID
		}
		if name := port.Component.Name; name != "" && id != "" {
			result[name] = id
		}
	}
	return result
}

func bgConnectionEntityID(entity *nifi.ConnectionEntity) string {
	if entity == nil {
		return ""
	}
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func decodeConnectionComponent(definition string) (nifi.ConnectionComponent, error) {
	var component nifi.ConnectionComponent
	if err := json.Unmarshal([]byte(definition), &component); err != nil {
		return nifi.ConnectionComponent{}, err
	}
	return component, nil
}

func renameFlowSnapshot(snapshot json.RawMessage, name string) (json.RawMessage, error) {
	var decoded map[string]any
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		return nil, err
	}
	flowContents, ok := decoded["flowContents"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("snapshot has no flowContents object")
	}
	flowContents["name"] = name
	return json.Marshal(decoded)
}
