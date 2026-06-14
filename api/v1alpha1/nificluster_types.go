package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ClusterMode string

const (
	ClusterModeInternal ClusterMode = "Internal"
	ClusterModeExternal ClusterMode = "External"
)

type NiFiClusterSpec struct {
	Mode           ClusterMode          `json:"mode,omitempty"`
	Image          string               `json:"image,omitempty"`
	Replicas       int32                `json:"replicas,omitempty"`
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiClusterStatus struct {
	CommonStatus       `json:",inline"`
	RootProcessGroupID string `json:"rootProcessGroupId,omitempty"`
	Endpoint           string `json:"endpoint,omitempty"`
}

type NiFiCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiClusterSpec   `json:"spec,omitempty"`
	Status            NiFiClusterStatus `json:"status,omitempty"`
}

type NiFiClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiCluster{}, &NiFiClusterList{})
}
