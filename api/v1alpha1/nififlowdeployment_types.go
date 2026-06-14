package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	Strategy string `json:"strategy,omitempty"`
}

type RollbackStrategy struct {
	Enabled   bool   `json:"enabled,omitempty"`
	OnFailure string `json:"onFailure,omitempty"`
}

type OwnershipPolicy struct {
	Mode  string `json:"mode,omitempty"`
	Prune bool   `json:"prune,omitempty"`
}

type NiFiFlowDeploymentSpec struct {
	ClusterRef          ClusterReference      `json:"clusterRef,omitempty"`
	Source              FlowDeploymentSource  `json:"source"`
	Target              FlowDeploymentTarget  `json:"target"`
	ParameterContextRef *LocalObjectReference `json:"parameterContextRef,omitempty"`
	Rollout             RolloutStrategy       `json:"rollout,omitempty"`
	Rollback            RollbackStrategy      `json:"rollback,omitempty"`
	Ownership           OwnershipPolicy       `json:"ownership,omitempty"`
	DeletionPolicy      DeletionPolicy        `json:"deletionPolicy,omitempty"`
	DriftPolicy         DriftPolicy           `json:"driftPolicy,omitempty"`
	AdoptionPolicy      AdoptionPolicy        `json:"adoptionPolicy,omitempty"`
	Reconciliation      ReconciliationPolicy  `json:"reconciliation,omitempty"`
}

type NiFiFlowDeploymentStatus struct {
	CommonStatus    `json:",inline"`
	DeployedVersion string `json:"deployedVersion,omitempty"`
	ArtifactDigest  string `json:"artifactDigest,omitempty"`
	ProcessGroupID  string `json:"processGroupId,omitempty"`
	SyncState       string `json:"syncState,omitempty"`
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
