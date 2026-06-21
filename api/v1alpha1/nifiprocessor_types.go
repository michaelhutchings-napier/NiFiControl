package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiProcessorSpec struct {
	ClusterRef            ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Type                        string                             `json:"type"`
	DisplayName                 string                             `json:"displayName,omitempty"`
	Bundle                      *ComponentBundle                   `json:"bundle,omitempty"`
	Position                    *Position                          `json:"position,omitempty"`
	Properties                  map[string]string                  `json:"properties,omitempty"`
	SensitiveProperties         map[string]SensitivePropertySource `json:"sensitiveProperties,omitempty"`
	AutoTerminatedRelationships []string                           `json:"autoTerminatedRelationships,omitempty"`
	Scheduling                  ComponentScheduling                `json:"scheduling,omitempty"`
	// +kubebuilder:validation:Enum=Running;Stopped;Disabled
	// +kubebuilder:default=Stopped
	State RuntimeState `json:"state,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiProcessorStatus struct {
	CommonStatus         `json:",inline"`
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
	ValidationStatus     string `json:"validationStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiProcessor struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiProcessorSpec   `json:"spec,omitempty"`
	Status            NiFiProcessorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiProcessorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiProcessor `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiProcessor{}, &NiFiProcessorList{})
}
