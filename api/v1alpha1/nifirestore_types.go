package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// NiFiRestoreSpec applies a captured flow snapshot into a cluster. The snapshot comes either
// from a NiFiBackup or from a ConfigMap/Secret holding a RegisteredFlowSnapshot under the
// flow.json key.
//
// +kubebuilder:validation:XValidation:rule="has(self.source.backupRef) != has(self.source.storageRef)",message="set exactly one of source.backupRef or source.storageRef"
type NiFiRestoreSpec struct {
	ClusterRef ClusterReference  `json:"clusterRef"`
	Source     NiFiRestoreSource `json:"source"`
	// TargetProcessGroupID is the process group to restore into. When empty the root process
	// group is targeted.
	TargetProcessGroupID string `json:"targetProcessGroupId,omitempty"`
	// Mode selects how the snapshot is applied. Import creates the snapshot as a new child
	// process group under the target (non-destructive). Replace replaces the target process
	// group's contents with the snapshot using NiFi's asynchronous replace workflow.
	// +kubebuilder:validation:Enum=Import;Replace
	// +kubebuilder:default=Import
	Mode string `json:"mode,omitempty"`
}

// NiFiRestoreSource references the flow snapshot to restore. Exactly one field is set.
type NiFiRestoreSource struct {
	// BackupRef is the name of a NiFiBackup in the same namespace.
	BackupRef string `json:"backupRef,omitempty"`
	// StorageRef references a ConfigMap or Secret holding the snapshot directly.
	StorageRef *NiFiBackupStorageSpec `json:"storageRef,omitempty"`
}

type NiFiRestoreStatus struct {
	CommonStatus `json:",inline"`
	// Phase is Pending, Succeeded, or Failed.
	Phase string `json:"phase,omitempty"`
	// RestoredProcessGroupID is the process group that received the snapshot (the new child
	// group for Import, or the target group for Replace).
	RestoredProcessGroupID string `json:"restoredProcessGroupId,omitempty"`
	// CompletedTime is when the restore finished.
	CompletedTime *metav1.Time `json:"completedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type NiFiRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiRestoreSpec   `json:"spec,omitempty"`
	Status            NiFiRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiRestore{}, &NiFiRestoreList{})
}
