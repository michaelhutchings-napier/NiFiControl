package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:validation:XValidation:rule="(has(self.git) ? 1 : 0) + (has(self.oci) ? 1 : 0) + (has(self.registry) ? 1 : 0) + (has(self.snapshot) ? 1 : 0) == 1",message="exactly one flow bundle source must be configured"
type FlowBundleSource struct {
	Git      *GitSource          `json:"git,omitempty"`
	OCI      *OCISource          `json:"oci,omitempty"`
	Registry *RegistryFlowSource `json:"registry,omitempty"`
	// Snapshot contains a NiFi RegisteredFlowSnapshot, such as the JSON
	// returned by downloading a versioned process group.
	// +kubebuilder:pruning:PreserveUnknownFields
	Snapshot *runtime.RawExtension `json:"snapshot,omitempty"`
}

type GitSource struct {
	// +kubebuilder:validation:MinLength=1
	URL  string `json:"url"`
	Ref  string `json:"ref,omitempty"`
	Path string `json:"path,omitempty"`
}

type OCISource struct {
	// +kubebuilder:validation:MinLength=1
	Image  string `json:"image"`
	Digest string `json:"digest,omitempty"`
	// Path is the snapshot file in the OCI image filesystem and defaults to flow.json.
	Path string `json:"path,omitempty"`
}

type RegistryFlowSource struct {
	RegistryClientRef LocalObjectReference `json:"registryClientRef"`
	// +kubebuilder:validation:MinLength=1
	BucketID string `json:"bucketId"`
	// +kubebuilder:validation:MinLength=1
	FlowID  string `json:"flowId"`
	Version string `json:"version,omitempty"`
}

type NiFiFlowBundleSpec struct {
	Source  FlowBundleSource `json:"source"`
	Version string           `json:"version,omitempty"`
}

type NiFiFlowBundleStatus struct {
	CommonStatus     `json:",inline"`
	ArtifactDigest   string `json:"artifactDigest,omitempty"`
	ResolvedRevision string `json:"resolvedRevision,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiFlowBundle struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiFlowBundleSpec   `json:"spec,omitempty"`
	Status            NiFiFlowBundleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiFlowBundleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiFlowBundle `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiFlowBundle{}, &NiFiFlowBundleList{})
}
