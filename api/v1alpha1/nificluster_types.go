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
// +kubebuilder:validation:XValidation:rule="!has(self.configOverrides) || self.mode != 'External'",message="configOverrides only applies to operator-managed (Internal) clusters"
// +kubebuilder:validation:XValidation:rule="!has(self.pod) || self.mode != 'External'",message="pod only applies to operator-managed (Internal) clusters"
// +kubebuilder:validation:XValidation:rule="!has(self.ports) || self.mode != 'External'",message="ports only applies to operator-managed (Internal) clusters"
// +kubebuilder:validation:XValidation:rule="!has(self.externalServices) || self.mode != 'External'",message="externalServices only applies to operator-managed (Internal) clusters"
// +kubebuilder:validation:XValidation:rule="!has(self.additionalProxyHosts) || self.mode != 'External'",message="additionalProxyHosts only applies to operator-managed (Internal) clusters"
// +kubebuilder:validation:XValidation:rule="!has(self.clusterDomain) || self.mode != 'External'",message="clusterDomain only applies to operator-managed (Internal) clusters"
// +kubebuilder:validation:XValidation:rule="!has(self.authentication) || (has(self.internalTLS) && self.internalTLS.enabled)",message="authentication requires internalTLS: NiFi only allows user authentication over HTTPS"
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
	Replicas int32                  `json:"replicas,omitempty"`
	API      *NiFiClusterAPISpec    `json:"api,omitempty"`
	Service  NiFiClusterServiceSpec `json:"service,omitempty"`
	// Ports customizes the network ports NiFi binds: the HTTP web port (non-TLS mode),
	// the node-to-node cluster protocol port, the site-to-site remote input port, and the
	// cluster load-balance port. Unset ports keep NiFi's defaults. The HTTPS web port is
	// configured through internalTLS.httpsPort. Only applies to Internal clusters.
	Ports *NiFiClusterPortsSpec `json:"ports,omitempty"`
	// ExternalServices provisions additional Kubernetes Services in front of the managed
	// nodes beyond the operator's own ClusterIP and headless Services — for example a
	// LoadBalancer for the web UI or a NodePort for site-to-site. Each targets the node
	// pods by the same selector. Only applies to Internal (operator-managed) clusters.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	ExternalServices []NiFiClusterExternalService `json:"externalServices,omitempty"`
	Storage          NiFiClusterStorageSpec       `json:"storage,omitempty"`
	Resources        corev1.ResourceRequirements  `json:"resources,omitempty"`
	JVM              NiFiClusterJVMSpec           `json:"jvm,omitempty"`
	Coordination     *NiFiClusterCoordinationSpec `json:"coordination,omitempty"`
	InternalTLS      *NiFiClusterInternalTLSSpec  `json:"internalTLS,omitempty"`
	Scheduling       *NiFiClusterScheduling       `json:"scheduling,omitempty"`
	// PodDisruptionBudget keeps a minimum number of NiFi nodes available during voluntary
	// disruptions such as node drains and certificate-rotation rolls.
	PodDisruptionBudget *NiFiClusterPDBSpec `json:"podDisruptionBudget,omitempty"`
	// Ingress exposes the managed NiFi cluster through a Kubernetes Ingress and configures
	// NiFi's allowed proxy host and context path accordingly.
	Ingress *NiFiClusterIngressSpec `json:"ingress,omitempty"`
	// AdditionalProxyHosts adds extra host[:port] entries to NiFi's nifi.web.proxy.host
	// allow-list, on top of the operator-computed Service DNS names and any Ingress host.
	// Set this for external load balancers or DNS names people reach NiFi through, otherwise
	// NiFi rejects those requests with an untrusted-proxy error. Additive: it never replaces
	// the computed entries. Only applies to Internal clusters.
	// +kubebuilder:validation:MaxItems=32
	AdditionalProxyHosts []ProxyHost `json:"additionalProxyHosts,omitempty"`
	// ClusterDomain is the Kubernetes cluster DNS domain used to build the fully-qualified
	// Service names in the node TLS certificate SANs and the operator-computed
	// nifi.web.proxy.host allow-list. Defaults to "cluster.local"; set it for clusters
	// configured with a non-default DNS domain. Only applies to Internal clusters.
	// +kubebuilder:validation:MaxLength=253
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// MaxTimerDrivenThreadCount sets NiFi's controller-level maximum timer-driven thread
	// count (nifi-api/controller/config), the thread pool that runs timer-driven
	// processors. It is a flow-level setting applied through the NiFi API once the cluster
	// is reachable, and enforced declaratively: the operator resets it if it drifts. Unset
	// leaves NiFi's default. Requires the operator to reach the cluster API (a secured
	// cluster uses the operator's mutual-TLS admin identity).
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	MaxTimerDrivenThreadCount *int32 `json:"maxTimerDrivenThreadCount,omitempty"`
	// Upgrade controls how managed NiFi version changes roll out across the StatefulSet.
	Upgrade *NiFiClusterUpgradeSpec `json:"upgrade,omitempty"`
	// ScaleDown controls how managed NiFi nodes are gracefully removed when replicas is
	// reduced, offloading each node's data through the NiFi cluster API before its pod is
	// deleted.
	ScaleDown *NiFiClusterScaleDownSpec `json:"scaleDown,omitempty"`
	// Metrics configures Prometheus metrics for the managed cluster, including an optional
	// Prometheus Operator ServiceMonitor pointing at NiFi's built-in metrics endpoint.
	Metrics       *NiFiClusterMetricsSpec `json:"metrics,omitempty"`
	AdditionalEnv []corev1.EnvVar         `json:"additionalEnv,omitempty"`
	// ConfigOverrides merges raw configuration entries into the managed nodes' NiFi
	// configuration files at startup, after the operator-managed settings, so an
	// override wins over the shipped default. It applies to every node in the cluster,
	// including NiFiNodeGroup pools. Only applies to Internal (operator-managed)
	// clusters.
	ConfigOverrides *NiFiClusterConfigOverrides `json:"configOverrides,omitempty"`
	// Pod customizes the generated node pods beyond the fields the API models
	// directly: extra metadata, image pull secrets, a ServiceAccount, sidecars, init
	// containers, and additional volumes (for example NAR extension or JDBC driver
	// libraries). It applies to every node in the cluster, including NiFiNodeGroup
	// pools. Only applies to Internal (operator-managed) clusters.
	Pod *NiFiClusterPodSpec `json:"pod,omitempty"`
	// Authentication configures how people log in to a secured managed cluster:
	// single-user credentials, LDAP, or OIDC. Requires internalTLS (NiFi only allows
	// authentication over HTTPS); the operator keeps authorizing itself and other
	// clients through mutual TLS regardless of the mode. Only applies to Internal
	// (operator-managed) clusters.
	Authentication *NiFiClusterAuthenticationSpec `json:"authentication,omitempty"`
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

// ProxyHost is a single host[:port] entry for NiFi's proxy allow-list. The length bound
// keeps the generated CRD within the API server's admission cost budget.
// +kubebuilder:validation:MaxLength=253
type ProxyHost string

// NiFiClusterPortsSpec customizes the network ports NiFi binds. An unset (zero) field
// keeps NiFi's default for that port.
type NiFiClusterPortsSpec struct {
	// HTTP is the plaintext web UI/API port NiFi binds in non-TLS mode. Default 8080.
	// Ignored when internalTLS is enabled; use internalTLS.httpsPort for the HTTPS port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	HTTP int32 `json:"http,omitempty"`
	// ClusterProtocol is the node-to-node cluster protocol port (nifi.cluster.node.protocol.port).
	// Default 11443. Only relevant for clustered deployments.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	ClusterProtocol int32 `json:"clusterProtocol,omitempty"`
	// RemoteInput is the site-to-site raw socket port (nifi.remote.input.socket.port).
	// Default 10000.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	RemoteInput int32 `json:"remoteInput,omitempty"`
	// LoadBalance is the cluster connection load-balance port (nifi.cluster.load.balance.port).
	// Default 6342. Only relevant for clustered deployments.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	LoadBalance int32 `json:"loadBalance,omitempty"`
}

// NiFiClusterExternalService describes an additional Service placed in front of the
// managed NiFi node pods. The operator sets the selector and owner reference; the Service
// is garbage-collected with the cluster and removed when dropped from the spec.
type NiFiClusterExternalService struct {
	// Name is the Service metadata.name. It must be a DNS-1035 label, unique in the
	// namespace, and distinct from the operator's own Service names.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=ClusterIP;NodePort;LoadBalancer
	// +kubebuilder:default=ClusterIP
	Type corev1.ServiceType `json:"type,omitempty"`
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	Ports                    []NiFiClusterExternalServicePort        `json:"ports"`
	Annotations              map[string]string                       `json:"annotations,omitempty"`
	Labels                   map[string]string                       `json:"labels,omitempty"`
	LoadBalancerIP           string                                  `json:"loadBalancerIP,omitempty"`
	LoadBalancerSourceRanges []string                                `json:"loadBalancerSourceRanges,omitempty"`
	ExternalTrafficPolicy    corev1.ServiceExternalTrafficPolicyType `json:"externalTrafficPolicy,omitempty"`
}

// NiFiClusterExternalServicePort is one port exposed by an external Service.
type NiFiClusterExternalServicePort struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=15
	Name string `json:"name"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
	// TargetPort is the container port this Service port routes to: a named NiFi container
	// port ("web", "cluster", "s2s", "load-balance") or a numeric port. Defaults to Port.
	TargetPort string `json:"targetPort,omitempty"`
	// NodePort requests a specific node port when the Service type is NodePort or LoadBalancer.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	NodePort int32 `json:"nodePort,omitempty"`
	// +kubebuilder:validation:Enum=TCP;UDP;SCTP
	// +kubebuilder:default=TCP
	Protocol corev1.Protocol `json:"protocol,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!has(self.repositories) || !has(self.enabled) || self.enabled",message="repositories requires persistent storage (storage.enabled)"
type NiFiClusterStorageSpec struct {
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`
	// +kubebuilder:default="10Gi"
	Size resource.Quantity `json:"size,omitempty"`
	// An empty value explicitly disables dynamic provisioning.
	StorageClassName *string                             `json:"storageClassName,omitempty"`
	AccessModes      []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
	// Repositories places individual NiFi repositories on dedicated PersistentVolumes —
	// for example the content repository on bulk storage while the flowfile repository
	// stays on fast disk. Repositories without an entry (and conf and local state)
	// remain on the main data volume. Adding or removing an entry on an existing
	// cluster recreates the StatefulSet (pods roll one at a time) and the affected
	// repository starts empty on its new volume: drain queues first, or take a backup,
	// because existing repository contents are not migrated.
	Repositories *NiFiClusterRepositoryStorageSpec `json:"repositories,omitempty"`
}

// NiFiClusterRepositoryStorageSpec selects which NiFi repositories get dedicated volumes.
type NiFiClusterRepositoryStorageSpec struct {
	// +optional
	FlowFile *NiFiClusterRepositoryVolumeSpec `json:"flowfile,omitempty"`
	// +optional
	Content *NiFiClusterRepositoryVolumeSpec `json:"content,omitempty"`
	// +optional
	Provenance *NiFiClusterRepositoryVolumeSpec `json:"provenance,omitempty"`
	// +optional
	Database *NiFiClusterRepositoryVolumeSpec `json:"database,omitempty"`
}

// NiFiClusterRepositoryVolumeSpec sizes one repository's dedicated PersistentVolume.
type NiFiClusterRepositoryVolumeSpec struct {
	// +kubebuilder:default="10Gi"
	Size resource.Quantity `json:"size,omitempty"`
	// An empty value explicitly disables dynamic provisioning; unset inherits the main
	// data volume's storage class.
	StorageClassName *string                             `json:"storageClassName,omitempty"`
	AccessModes      []corev1.PersistentVolumeAccessMode `json:"accessModes,omitempty"`
}

type NiFiClusterJVMSpec struct {
	// +kubebuilder:default="1g"
	HeapInitial string `json:"heapInitial,omitempty"`
	// +kubebuilder:default="1g"
	HeapMax string `json:"heapMax,omitempty"`
}

// NiFiClusterConfigOverrides merges raw configuration entries into the files the managed
// nodes boot with, for settings the API does not model directly (repository tuning,
// timeouts, custom extension properties, extra JVM arguments). Entries are applied after
// the operator-managed settings, so an override wins; removing an entry restores the
// NiFi image's shipped default on the next rollout. Keys the operator itself manages —
// the web listener, TLS keystores, the sensitive properties key, and cluster/ZooKeeper
// wiring — are rejected at admission because they have dedicated spec fields whose
// wiring an override would sever.
type NiFiClusterConfigOverrides struct {
	// NiFiProperties entries are merged into conf/nifi.properties on every node.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[A-Za-z0-9][A-Za-z0-9._-]*$'))",message="property names must start with an alphanumeric and contain only alphanumerics, dots, underscores, and hyphens"
	// +kubebuilder:validation:XValidation:rule="self.all(k, !self[k].contains('\\n') && !self[k].contains('\\r'))",message="property values must not contain newlines"
	// +kubebuilder:validation:XValidation:rule="self.all(k, !(k in ['nifi.web.http.host','nifi.web.http.port','nifi.web.https.host','nifi.web.https.port','nifi.security.keystore','nifi.security.keystoreType','nifi.security.keystorePasswd','nifi.security.keyPasswd','nifi.security.truststore','nifi.security.truststoreType','nifi.security.truststorePasswd','nifi.security.needClientAuth','nifi.security.user.authorizer','nifi.security.user.login.identity.provider','nifi.security.allow.anonymous.authentication','nifi.sensitive.props.key','nifi.cluster.is.node','nifi.cluster.node.address','nifi.cluster.node.protocol.port','nifi.cluster.protocol.is.secure','nifi.zookeeper.connect.string','nifi.zookeeper.root.node','nifi.remote.input.secure']))",message="this property is managed by the operator; use the corresponding spec field (web listener, internalTLS, coordination, or replicas) instead"
	NiFiProperties map[string]ConfigOverrideValue `json:"nifiProperties,omitempty"`
	// NiFiPropertiesFrom merges nifi.properties entries from Secrets, for values that
	// must not appear in the resource itself (an LDAP manager password, for example).
	// Each Secret's data keys are property names and its values are the property
	// values; Secrets are merged in list order, and inline nifiProperties entries win
	// over Secret-sourced ones. The same property-name rules and operator-managed-key
	// denylist apply, enforced when the cluster reconciles (the cluster reports
	// ConfigOverridesInvalid instead of being rejected at admission).
	// +optional
	// +kubebuilder:validation:MaxItems=8
	NiFiPropertiesFrom []corev1.LocalObjectReference `json:"nifiPropertiesFrom,omitempty"`
	// BootstrapProperties entries are merged into conf/bootstrap.conf the same way, for
	// example additional java.arg.N JVM arguments.
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^[A-Za-z0-9][A-Za-z0-9._-]*$'))",message="property names must start with an alphanumeric and contain only alphanumerics, dots, underscores, and hyphens"
	// +kubebuilder:validation:XValidation:rule="self.all(k, !self[k].contains('\\n') && !self[k].contains('\\r'))",message="property values must not contain newlines"
	// +kubebuilder:validation:XValidation:rule="self.all(k, !(k in ['java.arg.2','java.arg.3']))",message="heap arguments are managed by the operator; set spec.jvm instead"
	BootstrapProperties map[string]ConfigOverrideValue `json:"bootstrapProperties,omitempty"`
	// LogbackXml replaces conf/logback.xml wholesale with the given document, for
	// custom log levels, appenders, or retention. The content is not validated; a
	// malformed document surfaces as a NiFi startup failure. Removing it restores the
	// image's shipped logback.xml on the next rollout.
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	LogbackXml string `json:"logbackXml,omitempty"`
}

// ConfigOverrideValue is a single raw configuration value. The length bound keeps the
// CRD's CEL validation rules within the API server's admission cost budget.
// +kubebuilder:validation:MaxLength=2048
type ConfigOverrideValue string

// NiFiClusterAuthenticationSpec configures user authentication for a secured managed
// cluster. NiFi authenticates client certificates before any login provider, so the
// operator's mTLS access is unaffected by the mode. Identities that log in through the
// provider are authorized by the operator-managed file-based authorizer: seed them with
// adminIdentities, or manage them declaratively with NiFiUser / NiFiUserGroup /
// NiFiPolicy resources.
// +kubebuilder:validation:XValidation:rule="self.mode != 'SingleUser' || has(self.singleUser)",message="singleUser is required when mode is SingleUser"
// +kubebuilder:validation:XValidation:rule="self.mode != 'LDAP' || has(self.ldap)",message="ldap is required when mode is LDAP"
// +kubebuilder:validation:XValidation:rule="self.mode != 'OIDC' || has(self.oidc)",message="oidc is required when mode is OIDC"
type NiFiClusterAuthenticationSpec struct {
	// +kubebuilder:validation:Enum=SingleUser;LDAP;OIDC
	Mode string `json:"mode"`
	// +optional
	SingleUser *NiFiClusterSingleUserAuthSpec `json:"singleUser,omitempty"`
	// +optional
	LDAP *NiFiClusterLDAPAuthSpec `json:"ldap,omitempty"`
	// +optional
	OIDC *NiFiClusterOIDCAuthSpec `json:"oidc,omitempty"`
	// AdminIdentities are granted the full administrative policy set (flow, controller,
	// tenants, policies, system, counters, provenance, and the root process group) once
	// the cluster is reachable, so the listed people can administer NiFi from the UI
	// immediately. Identities must match what the provider yields — the single-user
	// username, the LDAP identity (per identityStrategy), or the OIDC claim value.
	// +optional
	// +kubebuilder:validation:MaxItems=16
	AdminIdentities []string `json:"adminIdentities,omitempty"`
}

// NiFiClusterSingleUserAuthSpec enables NiFi's single-user login provider with
// credentials from a Secret. Unlike stock NiFi, authorization still goes through the
// managed file-based authorizer, so list the username in adminIdentities (or grant it
// policies) for it to see anything.
type NiFiClusterSingleUserAuthSpec struct {
	// CredentialsSecretRef references a Secret with keys "username" and "password".
	// NiFi requires the password to be at least 12 characters. Rotating the Secret's
	// content rolls the nodes automatically.
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
}

// NiFiClusterLDAPAuthSpec enables NiFi's LDAP login provider.
type NiFiClusterLDAPAuthSpec struct {
	// URL of the directory server, for example ldap://openldap.auth.svc:389 or
	// ldaps://ldap.corp.example.com:636.
	// +kubebuilder:validation:Pattern=`^ldaps?://.+`
	URL string `json:"url"`
	// +kubebuilder:validation:Enum=SIMPLE;LDAPS;START_TLS
	// +kubebuilder:default=SIMPLE
	// AuthenticationStrategy for the directory connection. LDAPS and START_TLS trust
	// the JDK trust store unless caSecretRef supplies a private CA.
	AuthenticationStrategy string `json:"authenticationStrategy,omitempty"`
	// CASecretRef supplies a PEM CA bundle (default key ca.crt) that the operator builds
	// into a truststore so NiFi trusts a directory server whose LDAPS/START_TLS
	// certificate is signed by a private CA. Only meaningful with LDAPS or START_TLS.
	// Rotating the referenced Secret rolls the nodes.
	// +optional
	CASecretRef *SecretKeyRef `json:"caSecretRef,omitempty"`
	// ManagerDN binds for user lookups, for example cn=admin,dc=example,dc=org.
	ManagerDN string `json:"managerDN"`
	// ManagerPasswordSecretRef references the Secret key holding the manager password.
	ManagerPasswordSecretRef SecretKeyRef `json:"managerPasswordSecretRef"`
	// UserSearchBase, for example ou=people,dc=example,dc=org.
	UserSearchBase string `json:"userSearchBase"`
	// UserSearchFilter, for example (uid={0}) or (sAMAccountName={0}).
	UserSearchFilter string `json:"userSearchFilter"`
	// IdentityStrategy selects what becomes the NiFi identity: the full distinguished
	// name (USE_DN) or the login username (USE_USERNAME).
	// +kubebuilder:validation:Enum=USE_DN;USE_USERNAME
	// +kubebuilder:default=USE_USERNAME
	IdentityStrategy string `json:"identityStrategy,omitempty"`
}

// NiFiClusterOIDCAuthSpec enables NiFi's OpenID Connect login. The identity provider
// must allow the cluster's callback URL (https://<host>/nifi-api/access/oidc/callback).
type NiFiClusterOIDCAuthSpec struct {
	// DiscoveryURL of the provider, ending in /.well-known/openid-configuration.
	// +kubebuilder:validation:Pattern=`^https?://.+`
	DiscoveryURL string `json:"discoveryURL"`
	ClientID     string `json:"clientID"`
	// ClientSecretRef references the Secret key holding the OIDC client secret.
	ClientSecretRef SecretKeyRef `json:"clientSecretRef"`
	// Claim that identifies the user, for example email or preferred_username.
	// +kubebuilder:default=email
	Claim string `json:"claim,omitempty"`
	// +optional
	AdditionalScopes []string `json:"additionalScopes,omitempty"`
	// CASecretRef supplies a PEM CA bundle (default key ca.crt) so NiFi trusts an
	// identity provider whose HTTPS certificate is signed by a private CA. The operator
	// adds it to NiFi's truststore and switches the OIDC truststore strategy to NIFI.
	// Without it, NiFi trusts only the JDK CA bundle. Rotating the Secret rolls the nodes.
	// +optional
	CASecretRef *SecretKeyRef `json:"caSecretRef,omitempty"`
}

// NiFiClusterPodSpec customizes the node pods the operator generates. Operator-managed
// values always win: pod labels/annotations the operator sets (selector labels, config
// checksums) cannot be overridden, and the reserved volume and container names are
// rejected at admission.
type NiFiClusterPodSpec struct {
	// Labels are added to the node pods. Operator-managed labels take precedence.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations are added to the node pods. Operator-managed annotations take
	// precedence.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// ImagePullSecrets for pulling the NiFi image (and any sidecar images) from
	// private registries.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	// ServiceAccountName runs the node pods under a specific ServiceAccount.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
	// SecurityContext sets the pod-level security context for the node pods (for example
	// runAsUser, runAsGroup, fsGroup, seccompProfile). When set it replaces the operator
	// default, except that fsGroup defaults to 1000 (the apache/nifi image's uid/gid) when
	// left unset, so volume ownership stays correct.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`
	// ContainerSecurityContext sets the container-level security context on the operator's
	// own containers (the NiFi container and the initialize-data init container), for
	// restricted Pod Security Admission. allowPrivilegeEscalation: false, capabilities
	// drop ALL, runAsNonRoot, and seccompProfile all work with the stock apache/nifi image.
	// readOnlyRootFilesystem: true does NOT work on its own — NiFi writes under its install
	// directory (logs, work, run, nar_extensions, truststores) — so pair it with writable
	// emptyDir mounts over those paths via extraVolumes/extraVolumeMounts. Sidecars and
	// extra init containers carry their own securityContext.
	// +optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`
	// TerminationGracePeriodSeconds is how long Kubernetes waits after sending SIGTERM
	// before forcibly killing a node pod (SIGKILL). NiFi needs time to stop gracefully —
	// stop processors, checkpoint the flowfile repository, and flush the content and
	// provenance repositories — so the operator defaults this to 60 seconds (Kubernetes
	// itself defaults to only 30). Keep it comfortably above the NiFi bootstrap's
	// graceful.shutdown.seconds (20 by default); raise it for large repository backlogs or
	// when relying on node offload during scale-down. 0 forces an immediate SIGKILL with no
	// grace period (unsafe for a running flow).
	// +optional
	// +kubebuilder:validation:Minimum=0
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// Probes tunes the timing and thresholds of the operator's startup, liveness, and
	// readiness probes for the NiFi container. The probe actions stay operator-managed (which
	// NiFi endpoint is checked, and how TLS is handled); only the scheduling fields — periods,
	// timeouts, and thresholds — are adjustable. Widen the startup probe for flows that take
	// minutes to boot.
	// +optional
	Probes *NiFiClusterProbesSpec `json:"probes,omitempty"`
	// ExtraVolumes are appended to the pod volumes, for mounting NAR extensions,
	// driver libraries, or sidecar data.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	// +kubebuilder:validation:XValidation:rule="self.all(v, !(v.name in ['data','nificontrol-tls','nificontrol-config','nificontrol-overrides']))",message="volume names data, nificontrol-tls, nificontrol-config, and nificontrol-overrides are reserved for the operator"
	ExtraVolumes []corev1.Volume `json:"extraVolumes,omitempty"`
	// ExtraVolumeMounts are appended to the NiFi container's volume mounts, for
	// example mounting an extraVolumes NAR library at
	// /opt/nifi/nifi-current/nar_extensions.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	ExtraVolumeMounts []corev1.VolumeMount `json:"extraVolumeMounts,omitempty"`
	// ExtraInitContainers run after the operator's data initializer.
	// +optional
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:XValidation:rule="self.all(c, !(c.name in ['nifi','initialize-data']))",message="container names nifi and initialize-data are reserved for the operator"
	ExtraInitContainers []corev1.Container `json:"extraInitContainers,omitempty"`
	// ExtraContainers are appended as sidecars alongside the NiFi container.
	// +optional
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:XValidation:rule="self.all(c, !(c.name in ['nifi','initialize-data']))",message="container names nifi and initialize-data are reserved for the operator"
	ExtraContainers []corev1.Container `json:"extraContainers,omitempty"`
}

// NiFiClusterProbesSpec tunes the operator's startup, liveness, and readiness probes for the
// NiFi container. NiFi can take minutes to boot with large flows; widen the startup probe's
// failureThreshold/periodSeconds for slow starts, and the liveness/readiness probes for
// loaded or high-latency environments. Each probe's action is fixed by the operator; only
// the fields below are adjustable, and any left unset keep the operator default.
type NiFiClusterProbesSpec struct {
	// Startup tunes the startup probe, which gates the boot window before liveness and
	// readiness take over. Defaults: periodSeconds 10, timeoutSeconds 3 (5 for TLS),
	// failureThreshold 60 (a ~10-minute boot window).
	// +optional
	Startup *NiFiClusterProbeTuning `json:"startup,omitempty"`
	// Liveness tunes the liveness probe. Defaults: periodSeconds 20, timeoutSeconds 3,
	// failureThreshold 3.
	// +optional
	Liveness *NiFiClusterProbeTuning `json:"liveness,omitempty"`
	// Readiness tunes the readiness probe. Defaults: periodSeconds 10, timeoutSeconds 3
	// (5 for TLS), failureThreshold 3.
	// +optional
	Readiness *NiFiClusterProbeTuning `json:"readiness,omitempty"`
}

// NiFiClusterProbeTuning overrides the scheduling fields of one probe. Only fields that are
// set are applied; the rest keep the operator default. The probe action (httpGet/exec/
// tcpSocket against the correct NiFi endpoint) is not adjustable.
type NiFiClusterProbeTuning struct {
	// InitialDelaySeconds delays the first probe after the container starts. Usually left at
	// 0 for the startup probe, which already gates the boot window.
	// +optional
	// +kubebuilder:validation:Minimum=0
	InitialDelaySeconds *int32 `json:"initialDelaySeconds,omitempty"`
	// PeriodSeconds is how often the probe runs.
	// +optional
	// +kubebuilder:validation:Minimum=1
	PeriodSeconds *int32 `json:"periodSeconds,omitempty"`
	// TimeoutSeconds is how long each probe attempt may take before it is counted a failure.
	// +optional
	// +kubebuilder:validation:Minimum=1
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`
	// FailureThreshold is the number of consecutive failures before the probe is considered
	// failed. For the startup probe the boot window is periodSeconds * failureThreshold.
	// +optional
	// +kubebuilder:validation:Minimum=1
	FailureThreshold *int32 `json:"failureThreshold,omitempty"`
	// SuccessThreshold is the number of consecutive successes before the probe is considered
	// passed. Kubernetes requires this to be 1 for the liveness and startup probes.
	// +optional
	// +kubebuilder:validation:Minimum=1
	SuccessThreshold *int32 `json:"successThreshold,omitempty"`
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

// NiFiClusterMetricsSpec configures Prometheus metrics for a managed NiFi cluster. NiFi 2.x
// always serves metrics in Prometheus text format from its REST API at
// /nifi-api/flow/metrics/prometheus on the existing web port (the standalone
// PrometheusReportingTask was removed in NiFi 2.0), so the operator provisions no extra
// port or NiFi-side component. On a TLS-enabled cluster the endpoint requires
// client-certificate or bearer-token authentication; see docs/observability.md.
type NiFiClusterMetricsSpec struct {
	// Enabled turns on metrics handling for the managed cluster. When true the operator
	// records the metrics endpoint in status and, if serviceMonitor.enabled is set, renders
	// a Prometheus Operator ServiceMonitor.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`
	// Path is the HTTP path serving Prometheus-format metrics.
	// +kubebuilder:default="/nifi-api/flow/metrics/prometheus"
	Path string `json:"path,omitempty"`
	// ServiceMonitor controls rendering of a Prometheus Operator ServiceMonitor for the
	// managed cluster's metrics endpoint.
	ServiceMonitor *NiFiClusterServiceMonitorSpec `json:"serviceMonitor,omitempty"`
}

// NiFiClusterServiceMonitorSpec configures the Prometheus Operator ServiceMonitor that the
// operator renders for the managed cluster. The monitoring.coreos.com CRDs must be
// installed; if they are absent the cluster reports MetricsReady=False with reason
// CRDsNotInstalled and otherwise reconciles normally (metrics are best-effort, never fatal).
type NiFiClusterServiceMonitorSpec struct {
	// Enabled renders a ServiceMonitor selecting the managed cluster's Service.
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`
	// Interval is the Prometheus scrape interval, for example "30s". Empty uses the
	// Prometheus default.
	Interval string `json:"interval,omitempty"`
	// ScrapeTimeout bounds a single scrape, for example "10s". Empty uses the Prometheus
	// default.
	ScrapeTimeout string `json:"scrapeTimeout,omitempty"`
	// Labels are added to the ServiceMonitor metadata so a Prometheus instance's
	// serviceMonitorSelector can select it.
	Labels map[string]string `json:"labels,omitempty"`
	// InsecureSkipVerify disables TLS server-certificate verification when scraping a
	// TLS-enabled cluster. Intended for development only; by default the scrape trusts the
	// operator-managed CA.
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
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
	// External consumes PKCS12 keystores and PEM certificate material supplied outside the operator.
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

// NiFiExternalTLSSpec consumes externally supplied PKCS12 keystores and PEM certificate
// material. Each referenced Secret must contain keystore.p12, truststore.p12, tls.crt,
// and tls.key. ca.crt is optional; when present NiFiControl uses it to pin trust, otherwise
// the operator and readiness probe use the system trust store. The operator does not
// generate or rotate these materials.
type NiFiExternalTLSSpec struct {
	// ServerSecretName is the Secret holding the server/node keystore.p12, truststore.p12, tls.crt, and tls.key.
	// +kubebuilder:validation:MinLength=1
	ServerSecretName string `json:"serverSecretName"`
	// ClientSecretName is the Secret holding the operator client keystore.p12, truststore.p12,
	// tls.crt, and tls.key used by the operator's mTLS REST client.
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
	// Ready is true once required PKCS12 and PEM certificate material is available.
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

// NiFiClusterMetricsStatus reports the resolved metrics/observability state.
type NiFiClusterMetricsStatus struct {
	// Enabled mirrors spec.metrics.enabled.
	Enabled bool `json:"enabled,omitempty"`
	// Path is the metrics HTTP path scraped by Prometheus.
	Path string `json:"path,omitempty"`
	// ServiceMonitorName is the rendered Prometheus Operator ServiceMonitor, empty when none
	// is rendered (serviceMonitor disabled or the monitoring.coreos.com CRDs are absent).
	ServiceMonitorName string `json:"serviceMonitorName,omitempty"`
}

type NiFiClusterStatus struct {
	CommonStatus       `json:",inline"`
	RootProcessGroupID string                      `json:"rootProcessGroupId,omitempty"`
	Endpoint           string                      `json:"endpoint,omitempty"`
	Workload           *NiFiClusterWorkloadStatus  `json:"workload,omitempty"`
	TLS                *NiFiClusterTLSStatus       `json:"tls,omitempty"`
	ScaleDown          *NiFiClusterScaleDownStatus `json:"scaleDown,omitempty"`
	Metrics            *NiFiClusterMetricsStatus   `json:"metrics,omitempty"`
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
