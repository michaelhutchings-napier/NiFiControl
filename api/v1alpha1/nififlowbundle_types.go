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
// +kubebuilder:validation:XValidation:rule="has(self.clientCertificateSecretKeyRef) == has(self.clientKeySecretKeyRef)",message="clientCertificateSecretKeyRef and clientKeySecretKeyRef must be configured together"
// +kubebuilder:validation:XValidation:rule="!(has(self.oidc) && (has(self.tokenSecretKeyRef) || has(self.usernameSecretKeyRef) || has(self.passwordSecretKeyRef)))",message="oidc is mutually exclusive with token and username/password authentication"
// +kubebuilder:validation:XValidation:rule="!has(self.sshKnownHostsSecretKeyRef) || has(self.sshPrivateKeySecretKeyRef)",message="sshKnownHostsSecretKeyRef requires sshPrivateKeySecretKeyRef"
// +kubebuilder:validation:XValidation:rule="!has(self.sshPrivateKeyPassphraseSecretKeyRef) || has(self.sshPrivateKeySecretKeyRef)",message="sshPrivateKeyPassphraseSecretKeyRef requires sshPrivateKeySecretKeyRef"
type FlowArtifactCredentials struct {
	UsernameSecretKeyRef *SecretKeyRef `json:"usernameSecretKeyRef,omitempty"`
	PasswordSecretKeyRef *SecretKeyRef `json:"passwordSecretKeyRef,omitempty"`
	TokenSecretKeyRef    *SecretKeyRef `json:"tokenSecretKeyRef,omitempty"`
	CASecretKeyRef       *SecretKeyRef `json:"caSecretKeyRef,omitempty"`
	// SSHPrivateKeySecretKeyRef selects SSH authentication for an ssh:// or scp-style Git URL
	// (git@host:org/repo.git). The referenced value is a PEM-encoded SSH private key. SSH auth
	// applies to Git sources only.
	SSHPrivateKeySecretKeyRef *SecretKeyRef `json:"sshPrivateKeySecretKeyRef,omitempty"`
	// SSHPrivateKeyPassphraseSecretKeyRef decrypts an encrypted SSH private key.
	SSHPrivateKeyPassphraseSecretKeyRef *SecretKeyRef `json:"sshPrivateKeyPassphraseSecretKeyRef,omitempty"`
	// SSHKnownHostsSecretKeyRef provides an OpenSSH known_hosts file used to verify the Git
	// server's host key. Required for SSH Git unless sshInsecureIgnoreHostKey is set.
	SSHKnownHostsSecretKeyRef *SecretKeyRef `json:"sshKnownHostsSecretKeyRef,omitempty"`
	// SSHInsecureIgnoreHostKey disables SSH host-key verification. Development only: it exposes
	// the fetch to man-in-the-middle attacks. Prefer sshKnownHostsSecretKeyRef in production.
	SSHInsecureIgnoreHostKey bool `json:"sshInsecureIgnoreHostKey,omitempty"`
	// ClientCertificateSecretKeyRef and ClientKeySecretKeyRef present a PEM client certificate
	// for mutual TLS to an HTTPS NiFi Registry or OCI source. They are additive to token or
	// username/password authentication (the certificate authenticates the connection, the
	// token/credentials authenticate the request). Not used for Git.
	ClientCertificateSecretKeyRef *SecretKeyRef `json:"clientCertificateSecretKeyRef,omitempty"`
	ClientKeySecretKeyRef         *SecretKeyRef `json:"clientKeySecretKeyRef,omitempty"`
	// OIDC obtains a bearer token through the OAuth2 client-credentials grant and uses it to
	// authenticate to a NiFi Registry source. Mutually exclusive with token and
	// username/password; registry sources only.
	OIDC               *FlowArtifactOIDC `json:"oidc,omitempty"`
	InsecureSkipVerify bool              `json:"insecureSkipVerify,omitempty"`
}

// FlowArtifactOIDC configures the OAuth2 client-credentials grant used to obtain a bearer token
// for a NiFi Registry source.
type FlowArtifactOIDC struct {
	// TokenURL is the OAuth2 token endpoint (must be HTTPS).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^https://`
	TokenURL string `json:"tokenURL"`
	// ClientIDSecretKeyRef holds the OAuth2 client id.
	// +kubebuilder:validation:Required
	ClientIDSecretKeyRef *SecretKeyRef `json:"clientIDSecretKeyRef"`
	// ClientSecretSecretKeyRef holds the OAuth2 client secret.
	// +kubebuilder:validation:Required
	ClientSecretSecretKeyRef *SecretKeyRef `json:"clientSecretSecretKeyRef"`
	// Scopes requested in the token grant.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	Scopes []string `json:"scopes,omitempty"`
	// Audience requested in the token grant, sent as the `audience` form parameter for
	// providers that require it (Auth0, for example).
	// +optional
	Audience string `json:"audience,omitempty"`
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
