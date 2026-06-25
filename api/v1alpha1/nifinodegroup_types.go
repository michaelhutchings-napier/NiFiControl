package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NiFiNodeGroupSpec defines an additional, independently-scalable pool of NiFi nodes that
// join an existing operator-managed (Internal) cluster. All node groups share the cluster's
// headless Service, ZooKeeper coordination, sensitive-properties key, and TLS materials —
// they are peers of the cluster's primary nodes in one NiFi cluster — but can run with their
// own replica count, resources, JVM heap, storage, and scheduling. Each NiFiNodeGroup
// exposes a scale subresource so it can be autoscaled per tier with KEDA/HPA.
type NiFiNodeGroupSpec struct {
	// ClusterRef references the parent NiFiCluster (in the same namespace by default). The
	// cluster must be operator-managed (Internal) and clustered.
	ClusterRef ClusterReference `json:"clusterRef"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	Replicas int32 `json:"replicas,omitempty"`
	// Image overrides the NiFi image for this group; defaults to the cluster's image. All
	// nodes in a cluster must run the same NiFi version, so set this only to pin the same
	// version with a different repository/digest.
	Image string `json:"image,omitempty"`
	// Resources overrides the container resource requirements for this group's pods.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// JVM overrides the heap settings for this group; defaults to the cluster's JVM settings.
	JVM *NiFiClusterJVMSpec `json:"jvm,omitempty"`
	// Storage overrides persistent storage for this group; defaults to the cluster's storage.
	Storage *NiFiClusterStorageSpec `json:"storage,omitempty"`
	// Scheduling overrides pod placement for this group; defaults to the cluster's scheduling.
	Scheduling *NiFiClusterScheduling `json:"scheduling,omitempty"`
	// Upgrade overrides the StatefulSet update strategy for this group.
	Upgrade *NiFiClusterUpgradeSpec `json:"upgrade,omitempty"`
	// ScaleDown overrides the graceful scale-down policy for this group; defaults to the
	// cluster's policy.
	ScaleDown *NiFiClusterScaleDownSpec `json:"scaleDown,omitempty"`
	// AdditionalEnv appends environment variables to this group's pods (merged over the
	// cluster's additionalEnv).
	AdditionalEnv []corev1.EnvVar `json:"additionalEnv,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`
}

type NiFiNodeGroupStatus struct {
	CommonStatus `json:",inline"`
	// StatefulSetName is the StatefulSet backing this node group.
	StatefulSetName string `json:"statefulSetName,omitempty"`
	// Replicas is the current number of ready nodes in this group (backs the scale subresource).
	Replicas int32 `json:"replicas,omitempty"`
	// Selector is the serialized label selector matching this group's pods (backs the scale subresource).
	Selector string `json:"selector,omitempty"`
	// ScaleDown reports the node currently being offloaded during a graceful scale-down.
	ScaleDown *NiFiClusterScaleDownStatus `json:"scaleDown,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type NiFiNodeGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiNodeGroupSpec   `json:"spec,omitempty"`
	Status            NiFiNodeGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiNodeGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiNodeGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiNodeGroup{}, &NiFiNodeGroupList{})
}
