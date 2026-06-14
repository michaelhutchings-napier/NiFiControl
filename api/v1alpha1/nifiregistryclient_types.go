package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type RegistryClientType string

const (
	RegistryClientTypeNiFiRegistry RegistryClientType = "NiFiRegistry"
	RegistryClientTypeGitHub       RegistryClientType = "GitHub"
	RegistryClientTypeGitLab       RegistryClientType = "GitLab"
)

type NiFiRegistryClientSpec struct {
	ClusterRef     ClusterReference     `json:"clusterRef,omitempty"`
	Type           RegistryClientType   `json:"type,omitempty"`
	Description    string               `json:"description,omitempty"`
	URI            string               `json:"uri,omitempty"`
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiRegistryClientStatus struct {
	CommonStatus `json:",inline"`
	ResolvedType string `json:"resolvedType,omitempty"`
}

type NiFiRegistryClient struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiRegistryClientSpec   `json:"spec,omitempty"`
	Status            NiFiRegistryClientStatus `json:"status,omitempty"`
}

type NiFiRegistryClientList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiRegistryClient `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiRegistryClient{}, &NiFiRegistryClientList{})
}
