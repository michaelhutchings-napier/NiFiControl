package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type ConnectableType string

const (
	ConnectableTypeProcessor        ConnectableType = "Processor"
	ConnectableTypeInputPort        ConnectableType = "InputPort"
	ConnectableTypeOutputPort       ConnectableType = "OutputPort"
	ConnectableTypeFunnel           ConnectableType = "Funnel"
	ConnectableTypeRemoteInputPort  ConnectableType = "RemoteInputPort"
	ConnectableTypeRemoteOutputPort ConnectableType = "RemoteOutputPort"
)

type ConnectableReference struct {
	// +kubebuilder:validation:Enum=Processor;InputPort;OutputPort;Funnel;RemoteInputPort;RemoteOutputPort
	Type ConnectableType `json:"type"`
	// Name references the NiFiControl resource for this endpoint. For RemoteInputPort and
	// RemoteOutputPort it is the NiFiRemoteProcessGroup name, and PortName selects the remote port.
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	NiFiID    string `json:"nifiId,omitempty"`
	// PortName selects a remote port by name within the referenced NiFiRemoteProcessGroup. It is
	// only used when Type is RemoteInputPort or RemoteOutputPort.
	PortName string `json:"portName,omitempty"`
}

type NiFiConnectionSpec struct {
	ClusterRef                    ClusterReference      `json:"clusterRef"`
	ParentProcessGroupRef         ProcessGroupReference `json:"parentProcessGroupRef,omitempty"`
	Source                        ConnectableReference  `json:"source"`
	Destination                   ConnectableReference  `json:"destination"`
	SelectedRelationships         []string              `json:"selectedRelationships,omitempty"`
	BackPressureObjectThreshold   string                `json:"backPressureObjectThreshold,omitempty"`
	BackPressureDataSizeThreshold string                `json:"backPressureDataSizeThreshold,omitempty"`
	FlowFileExpiration            string                `json:"flowFileExpiration,omitempty"`
	Prioritizers                  []string              `json:"prioritizers,omitempty"`
	Bends                         []Position            `json:"bends,omitempty"`
	// +kubebuilder:validation:Enum=DoNotLoadBalance;PartitionByAttribute;RoundRobin;SingleNode
	LoadBalanceStrategy           string `json:"loadBalanceStrategy,omitempty"`
	LoadBalancePartitionAttribute string `json:"loadBalancePartitionAttribute,omitempty"`
	// +kubebuilder:validation:Enum=Delete;Orphan
	// +kubebuilder:default=Orphan
	DeletionPolicy DeletionPolicy       `json:"deletionPolicy,omitempty"`
	DriftPolicy    DriftPolicy          `json:"driftPolicy,omitempty"`
	AdoptionPolicy AdoptionPolicy       `json:"adoptionPolicy,omitempty"`
	Reconciliation ReconciliationPolicy `json:"reconciliation,omitempty"`
}

type NiFiConnectionStatus struct {
	CommonStatus  `json:",inline"`
	SourceID      string `json:"sourceId,omitempty"`
	DestinationID string `json:"destinationId,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type NiFiConnection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiConnectionSpec   `json:"spec,omitempty"`
	Status            NiFiConnectionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiConnection `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiConnection{}, &NiFiConnectionList{})
}
