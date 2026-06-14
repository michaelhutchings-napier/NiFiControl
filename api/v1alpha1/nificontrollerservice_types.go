package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiControllerServiceSpec struct {
	ClusterRef            ClusterReference                   `json:"clusterRef,omitempty"`
	ParentProcessGroupRef ProcessGroupReference              `json:"parentProcessGroupRef,omitempty"`
	Type                  string                             `json:"type"`
	Bundle                *ComponentBundle                   `json:"bundle,omitempty"`
	Properties            map[string]string                  `json:"properties,omitempty"`
	SensitiveProperties   map[string]SensitivePropertySource `json:"sensitiveProperties,omitempty"`
	ParameterContextRef   *LocalObjectReference              `json:"parameterContextRef,omitempty"`
	State                 RuntimeState                       `json:"state,omitempty"`
	DeletionPolicy        DeletionPolicy                     `json:"deletionPolicy,omitempty"`
	DriftPolicy           DriftPolicy                        `json:"driftPolicy,omitempty"`
	AdoptionPolicy        AdoptionPolicy                     `json:"adoptionPolicy,omitempty"`
	Reconciliation        ReconciliationPolicy               `json:"reconciliation,omitempty"`
}

type NiFiControllerServiceStatus struct {
	CommonStatus     `json:",inline"`
	ValidationStatus string `json:"validationStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiControllerService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiControllerServiceSpec   `json:"spec,omitempty"`
	Status            NiFiControllerServiceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiControllerServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiControllerService `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiControllerService{}, &NiFiControllerServiceList{})
}
