// Package keda provides a minimal, dependency-free typed surface over the keda.sh/v1alpha1
// ScaledObject resource. NiFiControl renders a ScaledObject as *unstructured.Unstructured
// rather than importing the KEDA Go module (whose pinned k8s.io versions conflict with this
// project's), which also lets the operator detect a missing KEDA installation gracefully
// (no REST mapping) rather than crashing. This mirrors the certmanager and prometheus packages.
package keda

import (
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the KEDA API group.
	GroupName = "keda.sh"
	// Version is the KEDA API version NiFiControl targets.
	Version = "v1alpha1"

	KindScaledObject = "ScaledObject"
	// KindTriggerAuthentication is the KEDA resource that supplies credentials to a trigger.
	KindTriggerAuthentication = "TriggerAuthentication"
)

// GroupVersion identifies the KEDA API surface used by NiFiControl.
var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// ScaledObjectGVK is the GroupVersionKind of the ScaledObject resource.
var ScaledObjectGVK = GroupVersion.WithKind(KindScaledObject)

// TriggerAuthenticationGVK is the GroupVersionKind of the TriggerAuthentication resource.
var TriggerAuthenticationGVK = GroupVersion.WithKind(KindTriggerAuthentication)

// ScaleTargetRef references the workload (or scalable custom resource) KEDA scales.
type ScaleTargetRef struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name"`
}

// Trigger is a single KEDA scaler. Metadata carries the scaler-specific configuration.
type Trigger struct {
	Type              string             `json:"type"`
	Name              string             `json:"name,omitempty"`
	Metadata          map[string]string  `json:"metadata"`
	AuthenticationRef *AuthenticationRef `json:"authenticationRef,omitempty"`
}

// AuthenticationRef points a trigger at a TriggerAuthentication (or ClusterTriggerAuthentication)
// that supplies its credentials.
type AuthenticationRef struct {
	Name string `json:"name"`
	Kind string `json:"kind,omitempty"`
}

// SecretTargetRef maps a KEDA trigger-auth parameter to a key in a Kubernetes Secret. KEDA (not
// the operator) reads the Secret, so the operator never handles the credential material.
type SecretTargetRef struct {
	Parameter string `json:"parameter"`
	Name      string `json:"name"`
	Key       string `json:"key"`
}

// TriggerAuthenticationSpec is the subset of the KEDA TriggerAuthentication spec NiFiControl sets.
type TriggerAuthenticationSpec struct {
	SecretTargetRef []SecretTargetRef `json:"secretTargetRef,omitempty"`
}

// HPAScalingPolicy is one scale policy (mirrors autoscaling/v2 HPAScalingPolicy).
type HPAScalingPolicy struct {
	Type          string `json:"type"`
	Value         int32  `json:"value"`
	PeriodSeconds int32  `json:"periodSeconds"`
}

// HPAScalingRules tunes one direction of scaling.
type HPAScalingRules struct {
	StabilizationWindowSeconds *int32             `json:"stabilizationWindowSeconds,omitempty"`
	Policies                   []HPAScalingPolicy `json:"policies,omitempty"`
}

// HPABehavior mirrors autoscaling/v2 HorizontalPodAutoscalerBehavior.
type HPABehavior struct {
	ScaleDown *HPAScalingRules `json:"scaleDown,omitempty"`
	ScaleUp   *HPAScalingRules `json:"scaleUp,omitempty"`
}

// HPAConfig customizes the HPA KEDA manages internally.
type HPAConfig struct {
	Behavior *HPABehavior `json:"behavior,omitempty"`
}

// Advanced holds advanced KEDA options.
type Advanced struct {
	HorizontalPodAutoscalerConfig *HPAConfig `json:"horizontalPodAutoscalerConfig,omitempty"`
}

// ScaledObjectSpec is the subset of the KEDA ScaledObject spec NiFiControl sets.
type ScaledObjectSpec struct {
	ScaleTargetRef  ScaleTargetRef `json:"scaleTargetRef"`
	MinReplicaCount *int32         `json:"minReplicaCount,omitempty"`
	MaxReplicaCount *int32         `json:"maxReplicaCount,omitempty"`
	PollingInterval *int32         `json:"pollingInterval,omitempty"`
	Advanced        *Advanced      `json:"advanced,omitempty"`
	Triggers        []Trigger      `json:"triggers"`
}

// New returns an empty unstructured ScaledObject, suitable as a target for client.Get or
// controllerutil.CreateOrUpdate.
func New() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(ScaledObjectGVK)
	return obj
}

// NewScaledObject builds an unstructured namespaced ScaledObject with the given labels and spec.
func NewScaledObject(name, namespace string, labels map[string]string, spec ScaledObjectSpec) (*unstructured.Unstructured, error) {
	specMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&spec)
	if err != nil {
		return nil, fmt.Errorf("convert ScaledObject spec: %w", err)
	}
	obj := New()
	obj.SetName(name)
	if namespace != "" {
		obj.SetNamespace(namespace)
	}
	if len(labels) > 0 {
		obj.SetLabels(labels)
	}
	if err := unstructured.SetNestedMap(obj.Object, specMap, "spec"); err != nil {
		return nil, fmt.Errorf("set ScaledObject spec: %w", err)
	}
	return obj, nil
}

// NewTriggerAuthentication returns an empty unstructured TriggerAuthentication, suitable as a
// target for client.Get, client.Delete, or controllerutil.CreateOrUpdate.
func NewTriggerAuthentication() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(TriggerAuthenticationGVK)
	return obj
}

// NewTriggerAuthenticationList returns an empty unstructured list for client.List of
// TriggerAuthentication resources.
func NewTriggerAuthenticationList() *unstructured.UnstructuredList {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(GroupVersion.WithKind(KindTriggerAuthentication + "List"))
	return list
}

// NewTriggerAuthenticationObject builds an unstructured namespaced TriggerAuthentication with the
// given labels and spec.
func NewTriggerAuthenticationObject(name, namespace string, labels map[string]string, spec TriggerAuthenticationSpec) (*unstructured.Unstructured, error) {
	specMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&spec)
	if err != nil {
		return nil, fmt.Errorf("convert TriggerAuthentication spec: %w", err)
	}
	obj := NewTriggerAuthentication()
	obj.SetName(name)
	if namespace != "" {
		obj.SetNamespace(namespace)
	}
	if len(labels) > 0 {
		obj.SetLabels(labels)
	}
	if err := unstructured.SetNestedMap(obj.Object, specMap, "spec"); err != nil {
		return nil, fmt.Errorf("set TriggerAuthentication spec: %w", err)
	}
	return obj, nil
}

// IsCRDNotInstalled reports whether err indicates the KEDA CRDs are absent from the cluster
// (no REST mapping for the ScaledObject kind). The operator surfaces this as a non-fatal
// status rather than treating it as a transient failure.
func IsCRDNotInstalled(err error) bool {
	if err == nil {
		return false
	}
	if meta.IsNoMatchError(err) {
		return true
	}
	// A scheme-backed client (notably the fake client used in tests) reports an unregistered
	// kind rather than a REST-mapping miss; treat it the same way.
	if runtime.IsNotRegisteredError(err) {
		return true
	}
	// The discovery client reports a missing resource type as a NotFound on the GroupResource;
	// an object NotFound carries a resource name, which lets us tell the two apart.
	if apierrors.IsNotFound(err) {
		var statusErr *apierrors.StatusError
		if errors.As(err, &statusErr) {
			details := statusErr.ErrStatus.Details
			return details != nil && details.Name == "" && details.Kind != ""
		}
	}
	return false
}
