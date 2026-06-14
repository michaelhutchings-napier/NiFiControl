package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type Parameter struct {
	// +kubebuilder:validation:MinLength=1
	Name               string                   `json:"name"`
	Value              string                   `json:"value,omitempty"`
	Description        string                   `json:"description,omitempty"`
	SensitiveValueFrom *SensitivePropertySource `json:"sensitiveValueFrom,omitempty"`
}

type NiFiParameterContextSpec struct {
	ClusterRef    ClusterReference       `json:"clusterRef,omitempty"`
	Description   string                 `json:"description,omitempty"`
	Parameters    []Parameter            `json:"parameters,omitempty"`
	InheritedRefs []LocalObjectReference `json:"inheritedRefs,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiParameterContextStatus struct {
	CommonStatus `json:",inline"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiParameterContext struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiParameterContextSpec   `json:"spec,omitempty"`
	Status            NiFiParameterContextStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiParameterContextList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiParameterContext `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiParameterContext{}, &NiFiParameterContextList{})
}
