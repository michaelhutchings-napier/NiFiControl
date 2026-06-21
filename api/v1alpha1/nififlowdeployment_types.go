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
	Enabled   bool   `json:"enabled,omitempty"`
	OnFailure string `json:"onFailure,omitempty"`
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
	ProcessGroupID       string                    `json:"processGroupId,omitempty"`
	SyncState            string                    `json:"syncState,omitempty"`
	LatestReplaceRequest *FlowReplaceRequestStatus `json:"latestReplaceRequest,omitempty"`
}

type FlowReplaceRequestStatus struct {
	ID               string `json:"id,omitempty"`
	State            string `json:"state,omitempty"`
	Complete         bool   `json:"complete,omitempty"`
	FailureReason    string `json:"failureReason,omitempty"`
	PercentCompleted int32  `json:"percentCompleted,omitempty"`
	TargetDigest     string `json:"targetDigest,omitempty"`
	TargetVersion    string `json:"targetVersion,omitempty"`
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
