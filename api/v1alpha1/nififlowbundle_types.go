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
	URL         string                   `json:"url"`
	Ref         string                   `json:"ref,omitempty"`
	Path        string                   `json:"path,omitempty"`
	Credentials *FlowArtifactCredentials `json:"credentials,omitempty"`
}

type OCISource struct {
	// +kubebuilder:validation:MinLength=1
	Image  string `json:"image"`
	Digest string `json:"digest,omitempty"`
	// Path is the snapshot file in the OCI image filesystem and defaults to flow.json.
	Path        string                   `json:"path,omitempty"`
	Credentials *FlowArtifactCredentials `json:"credentials,omitempty"`
	// Verify requires the image to carry a valid cosign signature before it is materialized.
	Verify *FlowArtifactVerification `json:"verify,omitempty"`
}

// FlowArtifactVerification requires an OCI flow artifact to carry a valid cosign signature before
// its snapshot is used. Only key-based verification (cosign "simple signing" with a public key) is
// supported; keyless verification (Fulcio/Rekor/OIDC) is not.
type FlowArtifactVerification struct {
	// CosignPublicKeySecretRef references a PEM-encoded public key (ECDSA, Ed25519, or RSA, such as
	// a cosign.pub) that must have signed the artifact.
	// +kubebuilder:validation:Required
	CosignPublicKeySecretRef *SecretKeyRef `json:"cosignPublicKeySecretRef"`
}

type RegistryFlowSource struct {
	RegistryClientRef LocalObjectReference `json:"registryClientRef"`
	// +kubebuilder:validation:MinLength=1
	BucketID string `json:"bucketId"`
	// +kubebuilder:validation:MinLength=1
	FlowID      string                   `json:"flowId"`
	Version     string                   `json:"version,omitempty"`
	Credentials *FlowArtifactCredentials `json:"credentials,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="has(self.usernameSecretKeyRef) == has(self.passwordSecretKeyRef)",message="username and password must be configured together"
// +kubebuilder:validation:XValidation:rule="!(has(self.tokenSecretKeyRef) && (has(self.usernameSecretKeyRef) || has(self.passwordSecretKeyRef)))",message="token and username/password authentication are mutually exclusive"
type FlowArtifactCredentials struct {
	UsernameSecretKeyRef *SecretKeyRef `json:"usernameSecretKeyRef,omitempty"`
	PasswordSecretKeyRef *SecretKeyRef `json:"passwordSecretKeyRef,omitempty"`
	TokenSecretKeyRef    *SecretKeyRef `json:"tokenSecretKeyRef,omitempty"`
	CASecretKeyRef       *SecretKeyRef `json:"caSecretKeyRef,omitempty"`
	InsecureSkipVerify   bool          `json:"insecureSkipVerify,omitempty"`
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
