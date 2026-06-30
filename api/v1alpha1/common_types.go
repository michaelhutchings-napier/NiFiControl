package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DeletionPolicy string
type DriftPolicyMode string
type AdoptionPolicyMode string
type RuntimeState string
type ConditionType string

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

	ConditionReady             ConditionType = "Ready"
	ConditionReconciling       ConditionType = "Reconciling"
	ConditionClusterReachable  ConditionType = "ClusterReachable"
	ConditionTLSReady          ConditionType = "TLSReady"
	ConditionMetricsReady      ConditionType = "MetricsReady"
	ConditionDependenciesReady ConditionType = "DependenciesReady"
	ConditionInSync            ConditionType = "InSync"
	ConditionDriftDetected     ConditionType = "DriftDetected"
	ConditionPaused            ConditionType = "Paused"
	ConditionError             ConditionType = "Error"
)

type LocalObjectReference struct {
	// +kubebuilder:validation:MinLength=1
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

type ClusterReference struct {
	// +kubebuilder:validation:MinLength=1
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
	// +kubebuilder:validation:Enum=Ignore;Warn;Reconcile;Fail
	// +kubebuilder:default=Warn
	Mode         DriftPolicyMode `json:"mode,omitempty"`
	IgnoreFields []string        `json:"ignoreFields,omitempty"`
}

type AdoptionPolicy struct {
	// +kubebuilder:validation:Enum=Never;IfExists;AdoptById;AdoptByName
	// +kubebuilder:default=Never
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

func (s *CommonStatus) SetCondition(conditionType ConditionType, status metav1.ConditionStatus, reason, message string, observedGeneration int64) {
	condition := metav1.Condition{
		Type:               string(conditionType),
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: observedGeneration,
	}
	metaSetStatusCondition(&s.Conditions, condition)
}

func (s *CommonStatus) MarkAccepted(observedGeneration int64) {
	s.ObservedGeneration = observedGeneration
	s.Ready = false
	s.Dependencies.Ready = true
	s.Dependencies.WaitingFor = nil
	s.Drift.Status = "Unknown"
	s.SetCondition(ConditionDependenciesReady, metav1.ConditionTrue, "DependenciesReady", "All declared dependencies are ready.", observedGeneration)
	s.SetCondition(ConditionReady, metav1.ConditionFalse, "ReconciliationPending", "NiFi-side reconciliation has not been implemented yet.", observedGeneration)
	s.SetCondition(ConditionReconciling, metav1.ConditionTrue, "Accepted", "The resource has been accepted by the NiFiControl controller.", observedGeneration)
}

func (s *CommonStatus) MarkReady(observedGeneration int64, reason, message string) {
	s.ObservedGeneration = observedGeneration
	s.Ready = true
	s.Dependencies.Ready = true
	s.Dependencies.WaitingFor = nil
	s.Drift.Status = "Unknown"
	s.SetCondition(ConditionDependenciesReady, metav1.ConditionTrue, "DependenciesReady", "All declared dependencies are ready.", observedGeneration)
	s.SetCondition(ConditionReady, metav1.ConditionTrue, reason, message, observedGeneration)
	s.SetCondition(ConditionReconciling, metav1.ConditionFalse, reason, message, observedGeneration)
}

func (s *CommonStatus) MarkNotReady(observedGeneration int64, reason, message string) {
	s.ObservedGeneration = observedGeneration
	s.Ready = false
	s.SetCondition(ConditionReady, metav1.ConditionFalse, reason, message, observedGeneration)
	s.SetCondition(ConditionReconciling, metav1.ConditionTrue, reason, message, observedGeneration)
}

func (s *CommonStatus) MarkWaitingForDependencies(observedGeneration int64, waitingFor []string) {
	s.ObservedGeneration = observedGeneration
	s.Ready = false
	s.Dependencies.Ready = false
	s.Dependencies.WaitingFor = waitingFor
	s.SetCondition(ConditionDependenciesReady, metav1.ConditionFalse, "DependenciesNotReady", "One or more dependencies are not ready.", observedGeneration)
	s.SetCondition(ConditionReady, metav1.ConditionFalse, "DependenciesNotReady", "The resource is waiting for dependencies.", observedGeneration)
	s.SetCondition(ConditionReconciling, metav1.ConditionTrue, "WaitingForDependencies", "The controller is waiting for dependencies before reconciling NiFi state.", observedGeneration)
}

func (s *CommonStatus) MarkDeleting(observedGeneration int64) {
	s.ObservedGeneration = observedGeneration
	s.Ready = false
	s.SetCondition(ConditionReady, metav1.ConditionFalse, "Deleting", "The resource is being deleted.", observedGeneration)
	s.SetCondition(ConditionReconciling, metav1.ConditionFalse, "Deleting", "The controller is removing finalizers.", observedGeneration)
}

func metaSetStatusCondition(conditions *[]metav1.Condition, newCondition metav1.Condition) {
	now := metav1.Now()
	newCondition.LastTransitionTime = now
	for i := range *conditions {
		existing := &(*conditions)[i]
		if existing.Type != newCondition.Type {
			continue
		}
		if existing.Status == newCondition.Status {
			newCondition.LastTransitionTime = existing.LastTransitionTime
		}
		*existing = newCondition
		return
	}
	*conditions = append(*conditions, newCondition)
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
