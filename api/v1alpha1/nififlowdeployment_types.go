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
	DeployedVersion      string                    `json:"deployedVersion,omitempty"`
	ArtifactDigest       string                    `json:"artifactDigest,omitempty"`
	DesiredContentDigest string                    `json:"desiredContentDigest,omitempty"`
	LiveContentDigest    string                    `json:"liveContentDigest,omitempty"`
	ProcessGroupID       string                    `json:"processGroupId,omitempty"`
	SyncState            string                    `json:"syncState,omitempty"`
	LatestReplaceRequest *FlowReplaceRequestStatus `json:"latestReplaceRequest,omitempty"`
	ActiveRollout        *FlowRolloutStatus        `json:"activeRollout,omitempty"`
	LastRollback         *FlowRollbackStatus       `json:"lastRollback,omitempty"`
	LastSuccessful       *FlowDeploymentHistory    `json:"lastSuccessfulDeployment,omitempty"`
	RolloutHistory       []FlowDeploymentHistory   `json:"rolloutHistory,omitempty"`
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
