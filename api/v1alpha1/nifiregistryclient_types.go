package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type RegistryClientType string

const (
	RegistryClientTypeNiFiRegistry RegistryClientType = "NiFiRegistry"
	RegistryClientTypeGitHub       RegistryClientType = "GitHub"
	RegistryClientTypeGitLab       RegistryClientType = "GitLab"
)

// GitHubFlowRegistrySpec configures a GitHubFlowRegistryClient (type: GitHub). It maps to NiFi's
// Repository Owner/Name/Path, Default Branch, and GitHub API URL properties. Supplying
// personalAccessTokenSecretRef selects Personal Access Token authentication; omit it for
// anonymous access to public repositories. App Installation auth and other advanced properties
// can be set through spec.properties / spec.sensitiveProperties.
type GitHubFlowRegistrySpec struct {
	// +kubebuilder:validation:MinLength=1
	RepositoryOwner string `json:"repositoryOwner"`
	// +kubebuilder:validation:MinLength=1
	RepositoryName string `json:"repositoryName"`
	RepositoryPath string `json:"repositoryPath,omitempty"`
	// DefaultBranch defaults to "main" when empty.
	DefaultBranch string `json:"defaultBranch,omitempty"`
	// APIURL defaults to "https://api.github.com/" when empty.
	APIURL                       string        `json:"apiUrl,omitempty"`
	PersonalAccessTokenSecretRef *SecretKeyRef `json:"personalAccessTokenSecretRef,omitempty"`
}

// GitLabFlowRegistrySpec configures a GitLabFlowRegistryClient (type: GitLab). GitLab supports
// Access Token authentication only; supply accessTokenSecretRef with a token that can read the
// repository.
type GitLabFlowRegistrySpec struct {
	// +kubebuilder:validation:MinLength=1
	RepositoryNamespace string `json:"repositoryNamespace"`
	// +kubebuilder:validation:MinLength=1
	RepositoryName string `json:"repositoryName"`
	RepositoryPath string `json:"repositoryPath,omitempty"`
	// DefaultBranch defaults to "main" when empty.
	DefaultBranch string `json:"defaultBranch,omitempty"`
	// APIURL defaults to "https://gitlab.com/" when empty.
	APIURL               string        `json:"apiUrl,omitempty"`
	AccessTokenSecretRef *SecretKeyRef `json:"accessTokenSecretRef,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="self.type != 'GitHub' || has(self.github)",message="github is required when type is GitHub"
// +kubebuilder:validation:XValidation:rule="self.type != 'GitLab' || has(self.gitlab)",message="gitlab is required when type is GitLab"
type NiFiRegistryClientSpec struct {
	ClusterRef ClusterReference `json:"clusterRef"`
	// +kubebuilder:validation:Enum=NiFiRegistry;GitHub;GitLab
	// +kubebuilder:default=NiFiRegistry
	Type        RegistryClientType `json:"type,omitempty"`
	Description string             `json:"description,omitempty"`
	// URI is the registry URL for the NiFiRegistry type (mapped to the "url" property).
	URI string `json:"uri,omitempty"`
	// GitHub configures a GitHub flow registry (required when type is GitHub).
	GitHub *GitHubFlowRegistrySpec `json:"github,omitempty"`
	// GitLab configures a GitLab flow registry (required when type is GitLab).
	GitLab *GitLabFlowRegistrySpec `json:"gitlab,omitempty"`
	// Properties sets additional NiFi component properties verbatim, overriding any derived from
	// the typed fields above (an escape hatch for advanced settings, e.g. GitHub App Installation).
	Properties map[string]string `json:"properties,omitempty"`
	// SensitiveProperties sets additional sensitive NiFi component properties from Secret values.
	SensitiveProperties map[string]SensitivePropertySource `json:"sensitiveProperties,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiRegistryClientStatus struct {
	CommonStatus `json:",inline"`
	ResolvedType string `json:"resolvedType,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiRegistryClient struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiRegistryClientSpec   `json:"spec,omitempty"`
	Status            NiFiRegistryClientStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiRegistryClientList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiRegistryClient `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiRegistryClient{}, &NiFiRegistryClientList{})
}
