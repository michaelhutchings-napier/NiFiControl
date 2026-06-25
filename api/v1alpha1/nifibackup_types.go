package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NiFiBackupSpec captures a point-in-time copy of a NiFi process group's flow
// configuration. The captured artifact is the same RegisteredFlowSnapshot that a
// NiFiRestore (or NiFiFlowDeployment) can later import or replace into a cluster. Backing up
// the persistent repositories (content/provenance/flowfile) is a storage-layer concern
// handled with CSI VolumeSnapshots; see docs/backup-restore.md.
type NiFiBackupSpec struct {
	ClusterRef ClusterReference `json:"clusterRef"`
	// ProcessGroupID is the NiFi process group to back up. When empty the root process group
	// (the entire flow) is captured.
	ProcessGroupID string `json:"processGroupId,omitempty"`
	// Storage selects where the captured flow snapshot is written.
	Storage NiFiBackupStorageSpec `json:"storage,omitempty"`
}

// NiFiBackupStorageSpec selects the Kubernetes object that holds a captured flow snapshot.
type NiFiBackupStorageSpec struct {
	// Type is the object kind used to store the snapshot.
	// +kubebuilder:validation:Enum=ConfigMap;Secret
	// +kubebuilder:default=ConfigMap
	Type string `json:"type,omitempty"`
	// Name of the ConfigMap or Secret. Defaults to "<backup-name>-flow".
	Name string `json:"name,omitempty"`
}

type NiFiBackupStatus struct {
	CommonStatus `json:",inline"`
	// Phase is Pending, Succeeded, or Failed.
	Phase string `json:"phase,omitempty"`
	// ProcessGroupID is the resolved process group that was captured.
	ProcessGroupID string `json:"processGroupId,omitempty"`
	// StorageRef is the name of the ConfigMap/Secret holding the snapshot.
	StorageRef string `json:"storageRef,omitempty"`
	// StorageType is ConfigMap or Secret.
	StorageType string `json:"storageType,omitempty"`
	// Digest is the SHA-256 (hex) of the captured snapshot bytes.
	Digest string `json:"digest,omitempty"`
	// SizeBytes is the size of the captured snapshot.
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// CompletedTime is when the backup finished.
	CompletedTime *metav1.Time `json:"completedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Stored",type=string,JSONPath=`.status.storageRef`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type NiFiBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiBackupSpec   `json:"spec,omitempty"`
	Status            NiFiBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiBackup{}, &NiFiBackupList{})
}
