package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiInputPortSpec struct {
	ClusterRef                       ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef            ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	DisplayName                      string                `json:"displayName,omitempty"`
	Position                         *Position             `json:"position,omitempty"`
	ConcurrentlySchedulableTaskCount int32                 `json:"concurrentlySchedulableTaskCount,omitempty"`
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

type NiFiInputPortStatus struct {
	CommonStatus         `json:",inline"`
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiInputPort struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiInputPortSpec   `json:"spec,omitempty"`
	Status            NiFiInputPortStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiInputPortList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiInputPort `json:"items"`
}

type NiFiOutputPortSpec struct {
	ClusterRef                       ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef            ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	DisplayName                      string                `json:"displayName,omitempty"`
	Position                         *Position             `json:"position,omitempty"`
	ConcurrentlySchedulableTaskCount int32                 `json:"concurrentlySchedulableTaskCount,omitempty"`
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

type NiFiOutputPortStatus struct {
	CommonStatus         `json:",inline"`
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiOutputPort struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiOutputPortSpec   `json:"spec,omitempty"`
	Status            NiFiOutputPortStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiOutputPortList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiOutputPort `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiInputPort{}, &NiFiInputPortList{}, &NiFiOutputPort{}, &NiFiOutputPortList{})
}
