package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NiFiParameterProviderSpec declares a NiFi parameter provider: a controller-level extension that
// sources parameter values from outside NiFi (environment variables, files on disk, a cloud secret
// manager, ...) so sensitive parameters can be managed and rotated externally. The operator creates
// and configures the provider over NiFi's REST API. A parameter provider is passive configuration —
// it has no run state; fetching its parameter groups and applying them to a parameter context is a
// separate action (see docs/parameter-providers.md).
type NiFiParameterProviderSpec struct {
	ClusterRef ClusterReference `json:"clusterRef"`
	// Type is the fully qualified provider class, for example
	// "org.apache.nifi.parameter.EnvironmentVariableParameterProvider".
	// +kubebuilder:validation:MinLength=1
	Type string `json:"type"`
	// Bundle pins the NAR bundle that supplies Type; omit it to let NiFi resolve a single
	// matching bundle.
	Bundle *ComponentBundle `json:"bundle,omitempty"`
	// Properties are the provider's non-sensitive properties.
	Properties map[string]string `json:"properties,omitempty"`
	// SensitiveProperties are provider properties whose values come from Kubernetes Secrets (for
	// example an access key for a cloud secret manager), so they stay out of the resource itself.
	SensitiveProperties map[string]SensitivePropertySource `json:"sensitiveProperties,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiParameterProviderStatus struct {
	CommonStatus     `json:",inline"`
	ValidationStatus string `json:"validationStatus,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiParameterProvider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiParameterProviderSpec   `json:"spec,omitempty"`
	Status            NiFiParameterProviderStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiParameterProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiParameterProvider `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiParameterProvider{}, &NiFiParameterProviderList{})
}
