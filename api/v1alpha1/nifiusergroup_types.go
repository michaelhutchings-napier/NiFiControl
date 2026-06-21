package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type UserGroupMember struct {
	UserRef LocalObjectReference `json:"userRef"`
}

type NiFiUserGroupSpec struct {
	ClusterRef ClusterReference `json:"clusterRef"`
	// +kubebuilder:validation:MinLength=1
	Identity string            `json:"identity"`
	Users    []UserGroupMember `json:"users,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiUserGroupStatus struct {
	CommonStatus `json:",inline"`
	MemberIDs    []string `json:"memberIds,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiUserGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiUserGroupSpec   `json:"spec,omitempty"`
	Status            NiFiUserGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiUserGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiUserGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiUserGroup{}, &NiFiUserGroupList{})
}
