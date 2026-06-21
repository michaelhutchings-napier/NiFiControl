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

// +kubebuilder:validation:XValidation:rule="has(self.bearerTokenSecretKeyRef) || (has(self.usernameSecretKeyRef) && has(self.passwordSecretKeyRef))",message="configure a bearer token or both username and password"
// +kubebuilder:validation:XValidation:rule="!(has(self.bearerTokenSecretKeyRef) && (has(self.usernameSecretKeyRef) || has(self.passwordSecretKeyRef)))",message="bearer token and username/password authentication are mutually exclusive"
type NiFiAPIAuthSpec struct {
	BearerTokenSecretKeyRef *SecretKeyRef `json:"bearerTokenSecretKeyRef,omitempty"`
	UsernameSecretKeyRef    *SecretKeyRef `json:"usernameSecretKeyRef,omitempty"`
	PasswordSecretKeyRef    *SecretKeyRef `json:"passwordSecretKeyRef,omitempty"`
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
