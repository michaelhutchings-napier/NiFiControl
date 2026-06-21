package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiReportingTaskSpec struct {
	ClusterRef ClusterReference `json:"clusterRef"`
	// +kubebuilder:validation:MinLength=1
	Type                string                             `json:"type"`
	Bundle              *ComponentBundle                   `json:"bundle,omitempty"`
	Properties          map[string]string                  `json:"properties,omitempty"`
	SensitiveProperties map[string]SensitivePropertySource `json:"sensitiveProperties,omitempty"`
	Scheduling          ComponentScheduling                `json:"scheduling,omitempty"`
	// +kubebuilder:validation:Enum=Enabled;Disabled
	// +kubebuilder:default=Disabled
	State RuntimeState `json:"state,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiReportingTaskStatus struct {
	CommonStatus     `json:",inline"`
	ValidationStatus string `json:"validationStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiReportingTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiReportingTaskSpec   `json:"spec,omitempty"`
	Status            NiFiReportingTaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiReportingTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiReportingTask `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiReportingTask{}, &NiFiReportingTaskList{})
}
