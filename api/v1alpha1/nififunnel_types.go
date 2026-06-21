package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiFunnelSpec struct {
	ClusterRef            ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	Position              *Position             `json:"position,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiFunnelStatus struct {
	CommonStatus         `json:",inline"`
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiFunnel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiFunnelSpec   `json:"spec,omitempty"`
	Status            NiFiFunnelStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiFunnelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiFunnel `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiFunnel{}, &NiFiFunnelList{})
}
