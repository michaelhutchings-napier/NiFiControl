package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	Replicas     int32                        `json:"replicas,omitempty"`
	API          *NiFiClusterAPISpec          `json:"api,omitempty"`
	Service      NiFiClusterServiceSpec       `json:"service,omitempty"`
	Storage      NiFiClusterStorageSpec       `json:"storage,omitempty"`
	Resources    corev1.ResourceRequirements  `json:"resources,omitempty"`
	JVM          NiFiClusterJVMSpec           `json:"jvm,omitempty"`
	Coordination *NiFiClusterCoordinationSpec `json:"coordination,omitempty"`
	InternalTLS  *NiFiClusterInternalTLSSpec  `json:"internalTLS,omitempty"`
	Scheduling   *NiFiClusterScheduling       `json:"scheduling,omitempty"`
	// PodDisruptionBudget keeps a minimum number of NiFi nodes available during voluntary
	// disruptions such as node drains and certificate-rotation rolls.
	PodDisruptionBudget *NiFiClusterPDBSpec `json:"podDisruptionBudget,omitempty"`
	// Ingress exposes the managed NiFi cluster through a Kubernetes Ingress and configures
	// NiFi's allowed proxy host and context path accordingly.
	Ingress *NiFiClusterIngressSpec `json:"ingress,omitempty"`
	// Upgrade controls how managed NiFi version changes roll out across the StatefulSet.
	Upgrade *NiFiClusterUpgradeSpec `json:"upgrade,omitempty"`
	// ScaleDown controls how managed NiFi nodes are gracefully removed when replicas is
	// reduced, offloading each node's data through the NiFi cluster API before its pod is
	// deleted.
	ScaleDown     *NiFiClusterScaleDownSpec `json:"scaleDown,omitempty"`
	AdditionalEnv []corev1.EnvVar           `json:"additionalEnv,omitempty"`
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

// NiFiClusterScheduling configures pod placement for the managed NiFi StatefulSet.
type NiFiClusterScheduling struct {
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	PriorityClassName         string                            `json:"priorityClassName,omitempty"`
}

// NiFiClusterPDBSpec configures a PodDisruptionBudget for the managed NiFi nodes. Exactly
// one of minAvailable or maxUnavailable may be set.
//
// +kubebuilder:validation:XValidation:rule="!self.enabled || !(has(self.minAvailable) && has(self.maxUnavailable))",message="set only one of minAvailable or maxUnavailable"
type NiFiClusterPDBSpec struct {
	// +kubebuilder:default=true
	Enabled        bool                `json:"enabled,omitempty"`
	MinAvailable   *intstr.IntOrString `json:"minAvailable,omitempty"`
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// NiFiClusterIngressSpec exposes the managed NiFi cluster through an Ingress.
type NiFiClusterIngressSpec struct {
	// +kubebuilder:default=true
	Enabled bool `json:"enabled,omitempty"`
	// IngressClassName selects the ingress controller.
	IngressClassName string `json:"ingressClassName,omitempty"`
	// Host is the external host name routed to NiFi. It is added to NiFi's allowed proxy
	// hosts so proxied requests are accepted.
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`
	// Path is the HTTP path routed to NiFi.
	// +kubebuilder:default="/"
	Path string `json:"path,omitempty"`
	// +kubebuilder:validation:Enum=Exact;Prefix;ImplementationSpecific
	// +kubebuilder:default=Prefix
	PathType string `json:"pathType,omitempty"`
	// ContextPath sets nifi.web.proxy.context.path when NiFi is served under a sub-path.
	ContextPath string            `json:"contextPath,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	// TLS configures Ingress TLS termination (the Secret is supplied by the user).
	TLS *NiFiClusterIngressTLS `json:"tls,omitempty"`
}

// NiFiClusterIngressTLS configures Ingress-level TLS termination.
type NiFiClusterIngressTLS struct {
	// +kubebuilder:validation:MinLength=1
	SecretName string   `json:"secretName"`
	Hosts      []string `json:"hosts,omitempty"`
}

// NiFiClusterUpgradeSpec controls how managed NiFi version changes roll out.
type NiFiClusterUpgradeSpec struct {
	// Strategy is the StatefulSet update strategy. RollingUpdate replaces nodes one at a
	// time; OnDelete waits for manual pod deletion, for fully controlled upgrades.
	// +kubebuilder:validation:Enum=RollingUpdate;OnDelete
	// +kubebuilder:default=RollingUpdate
	Strategy string `json:"strategy,omitempty"`
	// Partition holds back nodes with an ordinal below the partition during a rolling
	// upgrade, enabling staged/canary upgrades.
	// +kubebuilder:validation:Minimum=0
	Partition *int32 `json:"partition,omitempty"`
	// MinReadySeconds is how long a new node must be ready before the upgrade proceeds.
	// +kubebuilder:validation:Minimum=0
	MinReadySeconds int32 `json:"minReadySeconds,omitempty"`
}

// NiFiClusterScaleDownSpec controls graceful removal of NiFi nodes when replicas is
// reduced. The operator removes nodes from the highest ordinal down, one at a time:
// each node is disconnected and offloaded through the NiFi cluster API (redistributing its
// queued FlowFiles to the remaining nodes) and deleted from the cluster before its pod is
// removed.
type NiFiClusterScaleDownSpec struct {
	// OffloadData drains a node through the NiFi cluster offload API before its pod is
	// removed. When false, the StatefulSet shrinks immediately and queued FlowFiles on the
	// removed nodes are left on their persistent volumes.
	// +kubebuilder:default=true
	OffloadData *bool `json:"offloadData,omitempty"`
	// TimeoutSeconds bounds how long to wait for a single node to disconnect and offload
	// before applying OnTimeout.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=600
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
	// OnTimeout selects what happens when a node does not finish offloading within
	// TimeoutSeconds. Fail halts the scale-down and reports an error for operator
	// intervention; Force removes the node from the cluster and deletes its pod anyway,
	// which may strand any FlowFiles still queued on the node.
	// +kubebuilder:validation:Enum=Fail;Force
	// +kubebuilder:default=Fail
	OnTimeout ScaleDownTimeoutPolicy `json:"onTimeout,omitempty"`
}

// ScaleDownTimeoutPolicy selects behaviour when a node offload exceeds its timeout.
type ScaleDownTimeoutPolicy string

const (
	ScaleDownTimeoutFail  ScaleDownTimeoutPolicy = "Fail"
	ScaleDownTimeoutForce ScaleDownTimeoutPolicy = "Force"
)

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

// NiFiClusterScaleDownStatus reports the node currently being offloaded during a graceful
// scale-down.
type NiFiClusterScaleDownStatus struct {
	// NodeAddress is the NiFi cluster address of the node being removed.
	NodeAddress string `json:"nodeAddress,omitempty"`
	// Phase is Disconnecting, Offloading, or Removing.
	Phase string `json:"phase,omitempty"`
	// StartedAt is when offloading of the current node began (used for the offload timeout).
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
}

type NiFiClusterStatus struct {
	CommonStatus       `json:",inline"`
	RootProcessGroupID string                      `json:"rootProcessGroupId,omitempty"`
	Endpoint           string                      `json:"endpoint,omitempty"`
	Workload           *NiFiClusterWorkloadStatus  `json:"workload,omitempty"`
	TLS                *NiFiClusterTLSStatus       `json:"tls,omitempty"`
	ScaleDown          *NiFiClusterScaleDownStatus `json:"scaleDown,omitempty"`
	// Replicas is the current number of ready NiFi nodes. It backs the scale subresource so
	// HorizontalPodAutoscaler/KEDA can read the cluster's current size.
	Replicas int32 `json:"replicas,omitempty"`
	// Selector is the serialized label selector matching the managed NiFi pods. It backs the
	// scale subresource so per-pod metric autoscalers can find the pods.
	Selector string `json:"selector,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
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
