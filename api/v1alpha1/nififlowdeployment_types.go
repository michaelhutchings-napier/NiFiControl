package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +kubebuilder:validation:XValidation:rule="(has(self.bundleRef) ? 1 : 0) + (has(self.inline) ? 1 : 0) == 1",message="exactly one flow deployment source must be configured"
type FlowDeploymentSource struct {
	BundleRef *LocalObjectReference `json:"bundleRef,omitempty"`
	Version   string                `json:"version,omitempty"`
	Inline    *FlowBundleSource     `json:"inline,omitempty"`
}

type FlowDeploymentTarget struct {
	ParentProcessGroupRef ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	ProcessGroupName      string                `json:"processGroupName,omitempty"`
}

type RolloutStrategy struct {
	// +kubebuilder:validation:Enum=ApplyOnly;StopAllThenApply;ChangedOnly;Rolling;BlueGreen
	// +kubebuilder:default=ApplyOnly
	Strategy string `json:"strategy,omitempty"`
	// BlueGreen tunes the transactional BlueGreen rollout. It is ignored unless
	// strategy is BlueGreen.
	BlueGreen *BlueGreenStrategy `json:"blueGreen,omitempty"`
	// Readiness gates a completed rollout on the deployed flow being healthy (valid
	// components and enabled controller services) before it is marked in sync. It applies
	// to every strategy.
	Readiness *RolloutReadiness `json:"readiness,omitempty"`
	// QueuePolicy drains queues before a StopAllThenApply rollout stops the group.
	QueuePolicy *QueueDrainPolicy `json:"queuePolicy,omitempty"`
	// Retry bounds automatic re-attempts of a failed rollout.
	Retry *RolloutRetryPolicy `json:"retry,omitempty"`
	// Cancel requests cancellation of an in-flight rollout. A BlueGreen rollout switches
	// traffic back to blue; an in-place rollout cancels the NiFi replace request.
	Cancel bool `json:"cancel,omitempty"`
}

// RolloutReadiness gates a completed rollout on component health.
type RolloutReadiness struct {
	// RequireValidComponents waits until the deployed group reports no more than
	// maxUnavailable invalid components before the rollout is considered ready.
	// +kubebuilder:default=true
	RequireValidComponents *bool `json:"requireValidComponents,omitempty"`
	// RequireEnabledControllerServices enables and waits for the deployed group's
	// controller services before evaluating component validity.
	// +kubebuilder:default=true
	RequireEnabledControllerServices *bool `json:"requireEnabledControllerServices,omitempty"`
	// MaxUnavailable is the number of invalid components tolerated while still treating
	// the rollout as ready.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	MaxUnavailable int32 `json:"maxUnavailable,omitempty"`
	// TimeoutSeconds bounds how long to wait for readiness before failing the rollout.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=300
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// QueueDrainPolicy drains queues before a disruptive rollout step.
type QueueDrainPolicy struct {
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`
	// TimeoutSeconds bounds how long to wait for queues to drain.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=60
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
	// OnTimeout selects the behaviour when queues have not drained in time: Fail aborts
	// the rollout, Drop discards the remaining flow files, Proceed continues regardless.
	// +kubebuilder:validation:Enum=Fail;Drop;Proceed
	// +kubebuilder:default=Fail
	OnTimeout string `json:"onTimeout,omitempty"`
}

// RolloutRetryPolicy bounds automatic re-attempts of a failed rollout.
type RolloutRetryPolicy struct {
	// MaxRetries is the number of automatic re-attempts before the rollout is left failed
	// pending a spec change. Zero disables automatic retries.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	MaxRetries int32 `json:"maxRetries,omitempty"`
}

// BlueGreenStrategy configures transactional BlueGreen rollouts. A candidate (green)
// process group is deployed beside the live (blue) one, gated on readiness, and the
// external boundary connections are switched from blue's ports to green's matching ports
// before blue is retired. Boundary connection definitions are recorded so traffic can be
// switched back to blue on failure.
type BlueGreenStrategy struct {
	// DrainTimeoutSeconds bounds how long the operator waits for a boundary connection
	// queue to drain before applying onDrainTimeout.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=120
	DrainTimeoutSeconds int32 `json:"drainTimeoutSeconds,omitempty"`
	// OnDrainTimeout selects the behaviour when a boundary queue has not drained within
	// drainTimeoutSeconds: Fail aborts and switches traffic back to blue; Drop discards
	// the remaining flow files and proceeds.
	// +kubebuilder:validation:Enum=Fail;Drop
	// +kubebuilder:default=Fail
	OnDrainTimeout string `json:"onDrainTimeout,omitempty"`
	// ReadinessTimeoutSeconds bounds how long to wait for the candidate to become valid
	// (no invalid components) before failing the rollout.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=300
	ReadinessTimeoutSeconds int32 `json:"readinessTimeoutSeconds,omitempty"`
	// RequireEnabledControllerServices enables and waits for the candidate's controller
	// services before validation.
	// +kubebuilder:default=true
	RequireEnabledControllerServices *bool `json:"requireEnabledControllerServices,omitempty"`
}

type RollbackStrategy struct {
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:validation:Enum=PreviousSuccessful
	// +kubebuilder:default=PreviousSuccessful
	OnFailure string `json:"onFailure,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20
	// +kubebuilder:default=5
	HistoryLimit int32 `json:"historyLimit,omitempty"`
}

type OwnershipPolicy struct {
	// +kubebuilder:validation:Enum=Managed;InventoryOnly;Hybrid
	// +kubebuilder:default=Managed
	Mode  string `json:"mode,omitempty"`
	Prune bool   `json:"prune,omitempty"`
}

type NiFiFlowDeploymentSpec struct {
	ClusterRef          ClusterReference      `json:"clusterRef"`
	Source              FlowDeploymentSource  `json:"source"`
	Target              FlowDeploymentTarget  `json:"target"`
	ParameterContextRef *LocalObjectReference `json:"parameterContextRef,omitempty"`
	Rollout             RolloutStrategy       `json:"rollout,omitempty"`
	Rollback            RollbackStrategy      `json:"rollback,omitempty"`
	Ownership           OwnershipPolicy       `json:"ownership,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiFlowDeploymentStatus struct {
	CommonStatus         `json:",inline"`
	DeployedVersion      string `json:"deployedVersion,omitempty"`
	ArtifactDigest       string `json:"artifactDigest,omitempty"`
	DesiredContentDigest string `json:"desiredContentDigest,omitempty"`
	LiveContentDigest    string `json:"liveContentDigest,omitempty"`
	ProcessGroupID       string `json:"processGroupId,omitempty"`
	// RetiringProcessGroupID is a promoted-away (blue) process group kept for one
	// reconcile after a BlueGreen switch, then deleted.
	RetiringProcessGroupID string                    `json:"retiringProcessGroupId,omitempty"`
	SyncState              string                    `json:"syncState,omitempty"`
	LatestReplaceRequest   *FlowReplaceRequestStatus `json:"latestReplaceRequest,omitempty"`
	ActiveRollout          *FlowRolloutStatus        `json:"activeRollout,omitempty"`
	LastRollback           *FlowRollbackStatus       `json:"lastRollback,omitempty"`
	LastSuccessful         *FlowDeploymentHistory    `json:"lastSuccessfulDeployment,omitempty"`
	RolloutHistory         []FlowDeploymentHistory   `json:"rolloutHistory,omitempty"`
}

type FlowReplaceRequestStatus struct {
	ID                string `json:"id,omitempty"`
	State             string `json:"state,omitempty"`
	Complete          bool   `json:"complete,omitempty"`
	FailureReason     string `json:"failureReason,omitempty"`
	PercentCompleted  int32  `json:"percentCompleted,omitempty"`
	TargetDigest      string `json:"targetDigest,omitempty"`
	TargetVersion     string `json:"targetVersion,omitempty"`
	Operation         string `json:"operation,omitempty"`
	SnapshotConfigMap string `json:"snapshotConfigMap,omitempty"`
}

type FlowRolloutStatus struct {
	Phase           string      `json:"phase,omitempty"`
	Strategy        string      `json:"strategy,omitempty"`
	Operation       string      `json:"operation,omitempty"`
	TargetVersion   string      `json:"targetVersion,omitempty"`
	TargetDigest    string      `json:"targetDigest,omitempty"`
	PreviousVersion string      `json:"previousVersion,omitempty"`
	PreviousDigest  string      `json:"previousDigest,omitempty"`
	StartedAt       metav1.Time `json:"startedAt,omitempty"`
	// ReadinessStartedAt marks when the post-rollout readiness wait began.
	ReadinessStartedAt *metav1.Time `json:"readinessStartedAt,omitempty"`
	// RetryCount is the number of automatic re-attempts performed for this rollout.
	RetryCount int32 `json:"retryCount,omitempty"`
	// BlueGreen carries the transactional BlueGreen rollout state between reconciles.
	BlueGreen *BlueGreenRolloutStatus `json:"blueGreen,omitempty"`
}

// BlueGreenRolloutStatus tracks an in-progress transactional BlueGreen rollout so it can
// resume or roll back after an interruption.
type BlueGreenRolloutStatus struct {
	// CandidateProcessGroupID is the green process group being promoted.
	CandidateProcessGroupID string `json:"candidateProcessGroupId,omitempty"`
	// BlueProcessGroupID is the live process group being replaced.
	BlueProcessGroupID string `json:"blueProcessGroupId,omitempty"`
	// ParentProcessGroupID is the parent that owns the boundary connections.
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
	// Inventoried marks that the external boundary connections have been recorded.
	Inventoried bool `json:"inventoried,omitempty"`
	// ExternalConnections is the recorded boundary connection inventory used to switch
	// traffic to green and, on failure, switch it back to blue.
	ExternalConnections []ExternalConnectionRecord `json:"externalConnections,omitempty"`
}

// ExternalConnectionRecord captures a boundary connection crossing the deployment's
// process-group edge so it can be recreated against the green ports and, on rollback,
// restored against the blue ports.
type ExternalConnectionRecord struct {
	// Direction is Inbound (external source -> deployment input port) or Outbound
	// (deployment output port -> external destination).
	Direction string `json:"direction,omitempty"`
	// PortName is the deployment-side input or output port name used to match blue and
	// green ports.
	PortName string `json:"portName,omitempty"`
	// OriginalID is the connection id as it existed against blue.
	OriginalID string `json:"originalId,omitempty"`
	// Definition is the JSON connection component recorded for faithful recreation.
	Definition string `json:"definition,omitempty"`
	// Switched indicates the connection has been re-pointed to the green port.
	Switched bool `json:"switched,omitempty"`
	// GreenConnectionID is the id of the connection created against the green port.
	GreenConnectionID string `json:"greenConnectionId,omitempty"`
	// DrainStartedAt marks when draining of this connection began, bounding the wait
	// against the configured drain timeout.
	DrainStartedAt *metav1.Time `json:"drainStartedAt,omitempty"`
}

type FlowRollbackStatus struct {
	FailedGeneration int64        `json:"failedGeneration,omitempty"`
	FailedVersion    string       `json:"failedVersion,omitempty"`
	FailedDigest     string       `json:"failedDigest,omitempty"`
	RestoredVersion  string       `json:"restoredVersion,omitempty"`
	RestoredDigest   string       `json:"restoredDigest,omitempty"`
	CompletedAt      *metav1.Time `json:"completedAt,omitempty"`
	Message          string       `json:"message,omitempty"`
}

type FlowDeploymentHistory struct {
	Version           string      `json:"version,omitempty"`
	Digest            string      `json:"digest,omitempty"`
	SnapshotConfigMap string      `json:"snapshotConfigMap,omitempty"`
	Strategy          string      `json:"strategy,omitempty"`
	Result            string      `json:"result,omitempty"`
	Reason            string      `json:"reason,omitempty"`
	DeployedAt        metav1.Time `json:"deployedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiFlowDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiFlowDeploymentSpec   `json:"spec,omitempty"`
	Status            NiFiFlowDeploymentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiFlowDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiFlowDeployment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiFlowDeployment{}, &NiFiFlowDeploymentList{})
}
