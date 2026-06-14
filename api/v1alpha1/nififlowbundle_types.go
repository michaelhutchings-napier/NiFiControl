package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type FlowBundleSource struct {
	Git      *GitSource          `json:"git,omitempty"`
	OCI      *OCISource          `json:"oci,omitempty"`
	Registry *RegistryFlowSource `json:"registry,omitempty"`
}

type GitSource struct {
	URL  string `json:"url"`
	Ref  string `json:"ref,omitempty"`
	Path string `json:"path,omitempty"`
}

type OCISource struct {
	Image  string `json:"image"`
	Digest string `json:"digest,omitempty"`
}

type RegistryFlowSource struct {
	RegistryClientRef LocalObjectReference `json:"registryClientRef"`
	BucketID          string               `json:"bucketId"`
	FlowID            string               `json:"flowId"`
	Version           string               `json:"version,omitempty"`
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

type NiFiFlowBundle struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiFlowBundleSpec   `json:"spec,omitempty"`
	Status            NiFiFlowBundleStatus `json:"status,omitempty"`
}

type NiFiFlowBundleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiFlowBundle `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiFlowBundle{}, &NiFiFlowBundleList{})
}
