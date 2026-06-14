package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ClusterMode string

const (
	ClusterModeInternal ClusterMode = "Internal"
	ClusterModeExternal ClusterMode = "External"
)

type NiFiClusterSpec struct {
	// +kubebuilder:validation:Enum=Internal;External
	// +kubebuilder:default=Internal
	Mode  ClusterMode `json:"mode,omitempty"`
	Image string      `json:"image,omitempty"`
	// +kubebuilder:validation:Minimum=0
	Replicas int32               `json:"replicas,omitempty"`
	API      *NiFiClusterAPISpec `json:"api,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiClusterAPISpec struct {
	// +kubebuilder:validation:MinLength=1
	URI     string           `json:"uri"`
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}

type NiFiClusterStatus struct {
	CommonStatus       `json:",inline"`
	RootProcessGroupID string `json:"rootProcessGroupId,omitempty"`
	Endpoint           string `json:"endpoint,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiClusterSpec   `json:"spec,omitempty"`
	Status            NiFiClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiCluster{}, &NiFiClusterList{})
}
