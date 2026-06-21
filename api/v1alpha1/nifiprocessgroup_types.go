package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiProcessGroupSpec struct {
	ClusterRef            ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	DisplayName           string                `json:"displayName,omitempty"`
	Comments              string                `json:"comments,omitempty"`
	Position              *Position             `json:"position,omitempty"`
	ParameterContextRef   *LocalObjectReference `json:"parameterContextRef,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiProcessGroupStatus struct {
	CommonStatus         `json:",inline"`
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiProcessGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiProcessGroupSpec   `json:"spec,omitempty"`
	Status            NiFiProcessGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiProcessGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiProcessGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiProcessGroup{}, &NiFiProcessGroupList{})
}
