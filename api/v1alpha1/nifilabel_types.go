package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiLabelSpec struct {
	ClusterRef            ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Text     string            `json:"text"`
	Position *Position         `json:"position,omitempty"`
	Width    int32             `json:"width,omitempty"`
	Height   int32             `json:"height,omitempty"`
	Style    map[string]string `json:"style,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiLabelStatus struct {
	CommonStatus         `json:",inline"`
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiLabel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiLabelSpec   `json:"spec,omitempty"`
	Status            NiFiLabelStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiLabelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiLabel `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiLabel{}, &NiFiLabelList{})
}
