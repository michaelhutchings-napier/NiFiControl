package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NiFiPolicySpec declares grants on a NiFi access policy: a (resource, action) tuple granted to a
// set of user and user-group tenants. Access policies exist only on a secured NiFi with a managed
// authorizer. The granted tenants are referenced as NiFiUser / NiFiUserGroup resources so the
// operator can resolve their NiFi tenant ids. The operator preserves unrelated tenants already
// present on the same NiFi access policy.
type NiFiPolicySpec struct {
	ClusterRef ClusterReference `json:"clusterRef"`
	// Resource is the NiFi resource the policy applies to, for example "/flow", "/controller",
	// "/proxy", "/data/process-groups/{id}", "/policies", or "/tenants". It must start with "/".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^/`
	Resource string `json:"resource"`
	// Action is the access type granted.
	// +kubebuilder:validation:Enum=read;write
	Action string `json:"action"`
	// UserRefs grants the policy to these NiFiUser tenants (by resource name in this namespace
	// unless a namespace is set on the ref).
	UserRefs []LocalObjectReference `json:"userRefs,omitempty"`
	// UserGroupRefs grants the policy to these NiFiUserGroup tenants.
	UserGroupRefs []LocalObjectReference `json:"userGroupRefs,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

// NiFiPolicyStatus reports the resolved policy state.
type NiFiPolicyStatus struct {
	CommonStatus `json:",inline"`
	// UserIDs are the resolved user tenant ids granted the policy.
	UserIDs []string `json:"userIds,omitempty"`
	// UserGroupIDs are the resolved user-group tenant ids granted the policy.
	UserGroupIDs []string `json:"userGroupIds,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Resource",type=string,JSONPath=`.spec.resource`
// +kubebuilder:printcolumn:name="Action",type=string,JSONPath=`.spec.action`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type NiFiPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiPolicySpec   `json:"spec,omitempty"`
	Status            NiFiPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiPolicy{}, &NiFiPolicyList{})
}
