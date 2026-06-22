package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ClusterMode string

const (
	ClusterModeInternal ClusterMode = "Internal"
	ClusterModeExternal ClusterMode = "External"
)

// +kubebuilder:validation:XValidation:rule="self.mode != 'External' || has(self.api)",message="api is required when mode is External"
// +kubebuilder:validation:XValidation:rule="self.replicas <= 1 || has(self.coordination)",message="coordination is required when replicas is greater than one"
// +kubebuilder:validation:XValidation:rule="!has(self.internalTLS) || !self.internalTLS.enabled || self.mode != 'External'",message="internalTLS only applies to operator-managed (Internal) clusters"
type NiFiClusterSpec struct {
	// +kubebuilder:validation:Enum=Internal;External
	// +kubebuilder:default=Internal
	Mode ClusterMode `json:"mode,omitempty"`
	// +kubebuilder:default="apache/nifi:2.10.0"
	Image string `json:"image,omitempty"`
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	Replicas      int32                        `json:"replicas,omitempty"`
	API           *NiFiClusterAPISpec          `json:"api,omitempty"`
	Service       NiFiClusterServiceSpec       `json:"service,omitempty"`
	Storage       NiFiClusterStorageSpec       `json:"storage,omitempty"`
	Resources     corev1.ResourceRequirements  `json:"resources,omitempty"`
	JVM           NiFiClusterJVMSpec           `json:"jvm,omitempty"`
	Coordination  *NiFiClusterCoordinationSpec `json:"coordination,omitempty"`
	InternalTLS   *NiFiClusterInternalTLSSpec  `json:"internalTLS,omitempty"`
	AdditionalEnv []corev1.EnvVar              `json:"additionalEnv,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiClusterAPISpec struct {
	// +kubebuilder:validation:MinLength=1
	URI     string           `json:"uri"`
	Timeout *metav1.Duration `json:"timeout,omitempty"`
	TLS     *NiFiAPITLSSpec  `json:"tls,omitempty"`
	Auth    *NiFiAPIAuthSpec `json:"auth,omitempty"`
}

type NiFiAPITLSSpec struct {
	CASecretKeyRef     *SecretKeyRef `json:"caSecretKeyRef,omitempty"`
	ServerName         string        `json:"serverName,omitempty"`
	InsecureSkipVerify bool          `json:"insecureSkipVerify,omitempty"`
}

// NiFiAPIAuthSpec selects exactly one authentication mode for the operator's NiFi
// REST client. mTLS client-certificate, static bearer token, and username/password
// JWT exchange are mutually exclusive. Combined behaviour (for example presenting a
// client certificate for transport while exchanging a token for identity) is not yet
// proven against live NiFi 2.10 and is therefore rejected.
//
// +kubebuilder:validation:XValidation:rule="has(self.bearerTokenSecretKeyRef) || (has(self.usernameSecretKeyRef) && has(self.passwordSecretKeyRef)) || has(self.clientCertificate)",message="configure exactly one of: clientCertificate, bearer token, or username and password"
// +kubebuilder:validation:XValidation:rule="!(has(self.bearerTokenSecretKeyRef) && (has(self.usernameSecretKeyRef) || has(self.passwordSecretKeyRef) || has(self.clientCertificate)))",message="bearer token authentication is mutually exclusive with the other modes"
// +kubebuilder:validation:XValidation:rule="!(has(self.clientCertificate) && (has(self.usernameSecretKeyRef) || has(self.passwordSecretKeyRef)))",message="mTLS client certificate authentication is mutually exclusive with username/password"
type NiFiAPIAuthSpec struct {
	BearerTokenSecretKeyRef *SecretKeyRef             `json:"bearerTokenSecretKeyRef,omitempty"`
	UsernameSecretKeyRef    *SecretKeyRef             `json:"usernameSecretKeyRef,omitempty"`
	PasswordSecretKeyRef    *SecretKeyRef             `json:"passwordSecretKeyRef,omitempty"`
	ClientCertificate       *NiFiAPIClientCertificate `json:"clientCertificate,omitempty"`
}

// NiFiAPIClientCertificate references a Secret containing a PEM client certificate and
// private key that the operator presents to an external NiFi cluster for mTLS client
// authentication.
type NiFiAPIClientCertificate struct {
	// +kubebuilder:validation:MinLength=1
	SecretName string `json:"secretName"`
	// +kubebuilder:default="tls.crt"
	CertKey string `json:"certKey,omitempty"`
	// +kubebuilder:default="tls.key"
	KeyKey string `json:"keyKey,omitempty"`
}

type NiFiClusterServiceSpec struct {
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=8080
	Port int32 `json:"port,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	NodePort    int32             `json:"nodePort,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type NiFiClusterStorageSpec struct {
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`
	// +kubebuilder:default="10Gi"
	Size resource.Quantity `json:"size,omitempty"`
	// An empty value explicitly disables dynamic provisioning.
	StorageClassName *string                             `json:"storageClassName,omitempty"`
	AccessModes      []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

type NiFiClusterJVMSpec struct {
	// +kubebuilder:default="1g"
	HeapInitial string `json:"heapInitial,omitempty"`
	// +kubebuilder:default="1g"
	HeapMax string `json:"heapMax,omitempty"`
}

type NiFiClusterCoordinationSpec struct {
	// Required when replicas is greater than one.
	// +kubebuilder:validation:MinLength=1
	ZooKeeperConnectString string `json:"zookeeperConnectString"`
	// +kubebuilder:default="/nifi"
	ZooKeeperRootNode string `json:"zookeeperRootNode,omitempty"`
	// +kubebuilder:default="2 mins"
	ElectionMaxWait string `json:"electionMaxWait,omitempty"`
}

// NiFiClusterInternalTLSSpec configures operator-managed HTTPS and mutual TLS for an
// Internal (operator-managed) NiFi cluster. Exactly one certificate provider must be
// selected: an existing cert-manager issuerRef, an operator-managed self-signed CA
// chain, or externally supplied PKCS12 Secrets.
//
// +kubebuilder:validation:XValidation:rule="!self.enabled || [has(self.issuerRef), has(self.selfSigned), has(self.external)].exists_one(x, x)",message="when internalTLS is enabled, set exactly one of issuerRef, selfSigned, or external"
type NiFiClusterInternalTLSSpec struct {
	// Enabled turns on HTTPS and mutual TLS for the managed StatefulSet.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`
	// HTTPSPort is the secure web port exposed by NiFi and the managed Services.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=8443
	HTTPSPort int32 `json:"httpsPort,omitempty"`
	// IssuerRef references an existing cert-manager Issuer or ClusterIssuer that signs
	// the server and operator-client certificates directly.
	IssuerRef *CertManagerIssuerRef `json:"issuerRef,omitempty"`
	// SelfSigned requests an operator-managed, namespaced two-stage self-signed CA chain
	// (a self-signed Issuer that signs a CA certificate, backing a CA Issuer that signs
	// the leaf certificates).
	SelfSigned *NiFiSelfSignedCASpec `json:"selfSigned,omitempty"`
	// External consumes PKCS12 keystores and CA material supplied outside the operator.
	External *NiFiExternalTLSSpec `json:"external,omitempty"`
	// Certificate tunes the operator-generated leaf certificates. Ignored in external mode.
	Certificate *NiFiTLSCertificateSpec `json:"certificate,omitempty"`
}

// CertManagerIssuerRef references a cert-manager Issuer or ClusterIssuer.
type CertManagerIssuerRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	// +kubebuilder:default=Issuer
	Kind string `json:"kind,omitempty"`
	// +kubebuilder:default="cert-manager.io"
	Group string `json:"group,omitempty"`
}

// NiFiSelfSignedCASpec configures the operator-managed self-signed CA chain.
type NiFiSelfSignedCASpec struct {
	// CACommonName overrides the common name of the generated CA certificate.
	CACommonName string `json:"caCommonName,omitempty"`
	// CADuration is the validity period of the CA certificate (Go duration form).
	// +kubebuilder:default="8760h"
	CADuration string `json:"caDuration,omitempty"`
}

// NiFiExternalTLSSpec consumes externally supplied PKCS12 keystores. Each referenced
// Secret must contain keystore.p12, truststore.p12, and ca.crt. The operator does not
// generate or rotate these materials.
type NiFiExternalTLSSpec struct {
	// ServerSecretName is the Secret holding the server/node keystore.p12, truststore.p12, and ca.crt.
	// +kubebuilder:validation:MinLength=1
	ServerSecretName string `json:"serverSecretName"`
	// ClientSecretName is the Secret holding the operator client keystore.p12, ca.crt, plus
	// PEM tls.crt and tls.key used by the operator's mTLS REST client.
	// +kubebuilder:validation:MinLength=1
	ClientSecretName string `json:"clientSecretName"`
	// KeystorePasswordSecretRef references the password protecting the supplied PKCS12 stores.
	KeystorePasswordSecretRef *SecretKeyRef `json:"keystorePasswordSecretRef"`
	// InitialAdminIdentity is the exact NiFi identity (client certificate subject DN) granted initial admin.
	// +kubebuilder:validation:MinLength=1
	InitialAdminIdentity string `json:"initialAdminIdentity"`
	// NodeIdentity is the exact NiFi identity (server/node certificate subject DN) trusted as a cluster node.
	// +kubebuilder:validation:MinLength=1
	NodeIdentity string `json:"nodeIdentity"`
}

// NiFiTLSCertificateSpec tunes operator-generated leaf certificates.
type NiFiTLSCertificateSpec struct {
	// Duration is the validity period of the leaf certificates (Go duration form).
	// +kubebuilder:default="2160h"
	Duration string `json:"duration,omitempty"`
	// RenewBefore is how long before expiry cert-manager renews the leaf certificates.
	// +kubebuilder:default="360h"
	RenewBefore string `json:"renewBefore,omitempty"`
	// AdditionalServerSANs are extra DNS names added to the server/node certificate, for
	// example an Ingress host. Service and wildcard headless DNS names are always included.
	AdditionalServerSANs []string `json:"additionalServerSANs,omitempty"`
	// OperatorCommonName overrides the common name of the operator client certificate.
	// The resulting NiFi initial admin identity is "CN=<value>".
	OperatorCommonName string `json:"operatorCommonName,omitempty"`
	// NodeCommonName overrides the common name of the shared server/node certificate.
	// The resulting NiFi node identity is "CN=<value>".
	NodeCommonName string `json:"nodeCommonName,omitempty"`
}

// NiFiClusterTLSStatus reports the resolved internal TLS materials.
type NiFiClusterTLSStatus struct {
	// Mode is the resolved provider: CertManagerIssuer, SelfSigned, or External.
	Mode string `json:"mode,omitempty"`
	// IssuerName is the cert-manager issuer that signs the leaf certificates.
	IssuerName string `json:"issuerName,omitempty"`
	// IssuerKind is Issuer or ClusterIssuer.
	IssuerKind string `json:"issuerKind,omitempty"`
	// ServerSecretName is the Secret consumed by the NiFi StatefulSet.
	ServerSecretName string `json:"serverSecretName,omitempty"`
	// ClientSecretName is the Secret the operator loads for its mTLS REST client.
	ClientSecretName string `json:"clientSecretName,omitempty"`
	// InitialAdminIdentity is the NiFi identity granted initial admin (operator client DN).
	InitialAdminIdentity string `json:"initialAdminIdentity,omitempty"`
	// NodeIdentity is the NiFi identity trusted as a cluster node (server certificate DN).
	NodeIdentity string `json:"nodeIdentity,omitempty"`
	// Ready is true once keystore.p12, truststore.p12, and CA material are available.
	Ready bool `json:"ready,omitempty"`
}

type NiFiClusterWorkloadStatus struct {
	StatefulSetName string `json:"statefulSetName,omitempty"`
	ServiceName     string `json:"serviceName,omitempty"`
	HeadlessService string `json:"headlessServiceName,omitempty"`
	Replicas        int32  `json:"replicas,omitempty"`
	ReadyReplicas   int32  `json:"readyReplicas,omitempty"`
}

type NiFiClusterStatus struct {
	CommonStatus       `json:",inline"`
	RootProcessGroupID string                     `json:"rootProcessGroupId,omitempty"`
	Endpoint           string                     `json:"endpoint,omitempty"`
	Workload           *NiFiClusterWorkloadStatus `json:"workload,omitempty"`
	TLS                *NiFiClusterTLSStatus      `json:"tls,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiClusterSpec   `json:"spec,omitempty"`
	Status            NiFiClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiCluster{}, &NiFiClusterList{})
}
