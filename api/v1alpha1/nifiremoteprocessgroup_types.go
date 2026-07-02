package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type NiFiRemoteProcessGroupSpec struct {
	ClusterRef            ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	// TargetURIs are the site-to-site endpoints of the remote NiFi. NiFi tries them in order and
	// stores them as a single comma-separated value.
	// +kubebuilder:validation:MinItems=1
	TargetURIs []string `json:"targetUris"`
	// TransportProtocol selects the site-to-site transport. RAW uses NiFi's socket protocol; HTTP
	// tunnels site-to-site over the NiFi HTTP(S) port (useful through proxies).
	// +kubebuilder:validation:Enum=RAW;HTTP
	// +kubebuilder:default=RAW
	TransportProtocol string `json:"transportProtocol,omitempty"`
	// CommunicationsTimeout is a NiFi duration (e.g. "30 sec"); it must match NiFi's default so the
	// operator does not report perpetual drift when unset.
	// +kubebuilder:default="30 sec"
	CommunicationsTimeout string `json:"communicationsTimeout,omitempty"`
	// YieldDuration is a NiFi duration (e.g. "10 sec") the RPG waits before being scheduled again
	// after yielding.
	// +kubebuilder:default="10 sec"
	YieldDuration         string `json:"yieldDuration,omitempty"`
	LocalNetworkInterface string `json:"localNetworkInterface,omitempty"`
	Comments              string `json:"comments,omitempty"`
	// Proxy routes site-to-site traffic through an HTTP proxy (HTTP transport only).
	Proxy    *RemoteProcessGroupProxy `json:"proxy,omitempty"`
	Position *Position                `json:"position,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type RemoteProcessGroupProxy struct {
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32  `json:"port"`
	User string `json:"user,omitempty"`
	// PasswordSecretRef supplies the proxy password. NiFi masks it on read, so it is never compared
	// for drift once set.
	PasswordSecretRef *SecretKeyRef `json:"passwordSecretRef,omitempty"`
}

type NiFiRemoteProcessGroupStatus struct {
	CommonStatus         `json:",inline"`
	ParentProcessGroupID string `json:"parentProcessGroupId,omitempty"`
	// TransmissionStatus reflects whether the RPG is actively transmitting (Transmitting/Stopped).
	TransmissionStatus string `json:"transmissionStatus,omitempty"`
	// TargetSecure reports whether the remote NiFi requires a secure site-to-site connection.
	TargetSecure    bool  `json:"targetSecure,omitempty"`
	InputPortCount  int32 `json:"inputPortCount,omitempty"`
	OutputPortCount int32 `json:"outputPortCount,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=".status.ready"
// +kubebuilder:printcolumn:name="Transmission",type=string,JSONPath=".status.transmissionStatus"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type NiFiRemoteProcessGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiRemoteProcessGroupSpec   `json:"spec,omitempty"`
	Status            NiFiRemoteProcessGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiRemoteProcessGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiRemoteProcessGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiRemoteProcessGroup{}, &NiFiRemoteProcessGroupList{})
}
