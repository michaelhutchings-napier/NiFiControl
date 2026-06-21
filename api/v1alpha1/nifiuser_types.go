package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type UserCertificateSpec struct {
	Create     bool   `json:"create,omitempty"`
	SecretName string `json:"secretName,omitempty"`
}

type NiFiUserSpec struct {
	ClusterRef ClusterReference `json:"clusterRef"`
	// +kubebuilder:validation:MinLength=1
	Identity     string               `json:"identity"`
	Certificate  *UserCertificateSpec `json:"certificate,omitempty"`
	AuthProvider string               `json:"authProvider,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiUserStatus struct {
	CommonStatus          `json:",inline"`
	CertificateSecretName string `json:"certificateSecretName,omitempty"`
	CertificateReady      bool   `json:"certificateReady,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiUserSpec   `json:"spec,omitempty"`
	Status            NiFiUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiUser{}, &NiFiUserList{})
}
