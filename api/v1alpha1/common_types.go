package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DeletionPolicy string
type DriftPolicyMode string
type AdoptionPolicyMode string
type RuntimeState string

const (
	DeletionPolicyDelete DeletionPolicy = "Delete"
	DeletionPolicyOrphan DeletionPolicy = "Orphan"

	DriftPolicyIgnore    DriftPolicyMode = "Ignore"
	DriftPolicyWarn      DriftPolicyMode = "Warn"
	DriftPolicyReconcile DriftPolicyMode = "Reconcile"
	DriftPolicyFail      DriftPolicyMode = "Fail"

	AdoptionPolicyNever       AdoptionPolicyMode = "Never"
	AdoptionPolicyIfExists    AdoptionPolicyMode = "IfExists"
	AdoptionPolicyAdoptByID   AdoptionPolicyMode = "AdoptById"
	AdoptionPolicyAdoptByName AdoptionPolicyMode = "AdoptByName"

	RuntimeStateRunning  RuntimeState = "Running"
	RuntimeStateStopped  RuntimeState = "Stopped"
	RuntimeStateEnabled  RuntimeState = "Enabled"
	RuntimeStateDisabled RuntimeState = "Disabled"
)

type LocalObjectReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

type ClusterReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

type ProcessGroupReference struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Root      bool   `json:"root,omitempty"`
}

type SecretKeyRef struct {
	corev1.SecretKeySelector `json:",inline"`
}

type DriftPolicy struct {
	Mode         DriftPolicyMode `json:"mode,omitempty"`
	IgnoreFields []string        `json:"ignoreFields,omitempty"`
}

type AdoptionPolicy struct {
	Mode               AdoptionPolicyMode `json:"mode,omitempty"`
	NiFiID             string             `json:"nifiId,omitempty"`
	RequireAnnotation  bool               `json:"requireAnnotation,omitempty"`
	ReviewBeforeMutate bool               `json:"reviewBeforeMutate,omitempty"`
}

type ReconciliationPolicy struct {
	Paused bool `json:"paused,omitempty"`
}

type RevisionStatus struct {
	Version  int64  `json:"version,omitempty"`
	ClientID string `json:"clientId,omitempty"`
}

type DependencyStatus struct {
	Ready      bool     `json:"ready,omitempty"`
	WaitingFor []string `json:"waitingFor,omitempty"`
}

type DriftStatus struct {
	Status           string       `json:"status,omitempty"`
	LastDetectedTime *metav1.Time `json:"lastDetectedTime,omitempty"`
	Differences      []string     `json:"differences,omitempty"`
}

type SyncStatus struct {
	LastAttemptTime    *metav1.Time `json:"lastAttemptTime,omitempty"`
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`
	LastError          string       `json:"lastError,omitempty"`
}

type CommonStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Ready              bool               `json:"ready,omitempty"`
	NiFiID             string             `json:"nifiId,omitempty"`
	Revision           RevisionStatus     `json:"revision,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Dependencies       DependencyStatus   `json:"dependencies,omitempty"`
	Drift              DriftStatus        `json:"drift,omitempty"`
	Sync               SyncStatus         `json:"sync,omitempty"`
}

type Position struct {
	X int64 `json:"x,omitempty"`
	Y int64 `json:"y,omitempty"`
}

type SensitivePropertySource struct {
	SecretKeyRef *SecretKeyRef `json:"secretKeyRef,omitempty"`
}

type ComponentBundle struct {
	Group    string `json:"group,omitempty"`
	Artifact string `json:"artifact,omitempty"`
	Version  string `json:"version,omitempty"`
}

type ComponentScheduling struct {
	Strategy                         string `json:"strategy,omitempty"`
	Period                           string `json:"period,omitempty"`
	ConcurrentlySchedulableTaskCount int32  `json:"concurrentlySchedulableTaskCount,omitempty"`
}
