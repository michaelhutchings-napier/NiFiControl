package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NiFiAutoscalerSpec configures autoscaling for a scalable NiFi resource (a NiFiCluster or a
// NiFiNodeGroup). NiFiControl does not implement its own metrics polling loop or scaling
// algorithm: a NiFiAutoscaler renders a KEDA ScaledObject (for Prometheus/external metrics)
// or a native HorizontalPodAutoscaler (for CPU/memory) that drives the target's scale
// subresource. Because scaling targets the scale subresource rather than the StatefulSet,
// scale-downs run the operator's graceful node offload. The autoscaler adds a NiFi-aware UX:
// safe defaults, validation, and a single status surface.
//
// +kubebuilder:validation:XValidation:rule="self.maxReplicas >= self.minReplicas",message="maxReplicas must be greater than or equal to minReplicas"
type NiFiAutoscalerSpec struct {
	// Target is the scalable NiFi resource this autoscaler drives, in the same namespace.
	Target NiFiAutoscalerTarget `json:"target"`
	// MinReplicas is the lower bound. It must be at least 1: the operator never scales a
	// clustered NiFi to zero (that would destroy the cluster).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	MinReplicas int32 `json:"minReplicas,omitempty"`
	// MaxReplicas is the upper bound.
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas"`
	// Metrics are the scaling signals. A Prometheus metric renders a KEDA ScaledObject (KEDA
	// must be installed); a Resource (cpu/memory) metric renders a native HorizontalPodAutoscaler.
	// All metrics must be the same family (all Prometheus, or a single Resource metric).
	// +kubebuilder:validation:MinItems=1
	Metrics []NiFiAutoscalerMetric `json:"metrics"`
	// Behavior tunes NiFi-safe scale-down. NiFi node offload is comparatively expensive, so the
	// defaults are deliberately conservative (one node at a time, a long stabilization window).
	Behavior *NiFiAutoscalerBehavior `json:"behavior,omitempty"`
	// PollingInterval is how often KEDA evaluates the metrics, in seconds (KEDA-rendered
	// autoscalers only). Empty uses the KEDA default.
	// +kubebuilder:validation:Minimum=1
	PollingInterval *int32 `json:"pollingInterval,omitempty"`
}

// NiFiAutoscalerTarget references the scalable NiFi resource. Both NiFiCluster and
// NiFiNodeGroup expose a scale subresource.
type NiFiAutoscalerTarget struct {
	// +kubebuilder:validation:Enum=NiFiCluster;NiFiNodeGroup
	// +kubebuilder:default=NiFiCluster
	Kind string `json:"kind,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// NiFiAutoscalerMetric is one scaling signal. Exactly one source matching Type must be set.
//
// +kubebuilder:validation:XValidation:rule="self.type != 'Prometheus' || has(self.prometheus)",message="prometheus is required when type is Prometheus"
// +kubebuilder:validation:XValidation:rule="self.type != 'Resource' || has(self.resource)",message="resource is required when type is Resource"
type NiFiAutoscalerMetric struct {
	// +kubebuilder:validation:Enum=Prometheus;Resource
	Type string `json:"type"`
	// Prometheus scales on a PromQL query (rendered as a KEDA prometheus trigger). NiFi 2.x
	// serves metrics such as nifi_amount_items_queued at /nifi-api/flow/metrics/prometheus; see
	// docs/observability.md for getting those into Prometheus.
	Prometheus *PrometheusMetricSource `json:"prometheus,omitempty"`
	// Resource scales on pod CPU or memory utilization (rendered as a native HPA resource metric).
	Resource *ResourceMetricSource `json:"resource,omitempty"`
}

// PrometheusMetricSource configures a KEDA prometheus scaling trigger.
type PrometheusMetricSource struct {
	// ServerAddress is the Prometheus query endpoint, e.g. http://prometheus.monitoring.svc:9090.
	// +kubebuilder:validation:MinLength=1
	ServerAddress string `json:"serverAddress"`
	// Query is the PromQL query whose scalar result drives scaling, e.g.
	// sum(nifi_amount_items_queued).
	// +kubebuilder:validation:MinLength=1
	Query string `json:"query"`
	// Threshold is the target value per replica (KEDA scales replicas = ceil(query/threshold),
	// bounded by min/max). Expressed as a string to allow fractional values.
	// +kubebuilder:validation:MinLength=1
	Threshold string `json:"threshold"`
	// Name optionally names the trigger (defaults to the metric index).
	Name string `json:"name,omitempty"`
}

// ResourceMetricSource configures a native HPA resource metric.
type ResourceMetricSource struct {
	// +kubebuilder:validation:Enum=cpu;memory
	Name string `json:"name"`
	// TargetAverageUtilization is the target average utilization across pods, as a percentage.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetAverageUtilization int32 `json:"targetAverageUtilization"`
}

// NiFiAutoscalerScaleDownStrategy selects which node a scale-down removes.
type NiFiAutoscalerScaleDownStrategy string

const (
	// ScaleDownHighestOrdinal removes the highest-ordinal node first. This is how the operator
	// offloads today (StatefulSet semantics), and it keeps the coordinator-eligible ordinal 0
	// until last.
	ScaleDownHighestOrdinal NiFiAutoscalerScaleDownStrategy = "HighestOrdinal"
	// ScaleDownNonPrimary avoids removing the primary/coordinator node. With highest-ordinal-first
	// removal this is equivalent to HighestOrdinal until only one node remains; it is offered as
	// an explicit intent.
	ScaleDownNonPrimary NiFiAutoscalerScaleDownStrategy = "NonPrimary"
	// ScaleDownLeastBusy would remove the least-loaded node. It is not yet supported: selecting an
	// arbitrary (non-highest-ordinal) node requires pod-level management rather than a StatefulSet.
	ScaleDownLeastBusy NiFiAutoscalerScaleDownStrategy = "LeastBusy"
)

// NiFiAutoscalerBehavior tunes scale-down. These map to the rendered HPA scaleDown behavior;
// the operator's graceful offload still applies on every replica decrease.
type NiFiAutoscalerBehavior struct {
	// ScaleDownStrategy selects which node a scale-down removes. The operator currently always
	// offloads from the highest ordinal down, so HighestOrdinal and NonPrimary describe the
	// existing behavior; LeastBusy is reserved and rejected until pod-level management exists.
	// +kubebuilder:validation:Enum=HighestOrdinal;NonPrimary;LeastBusy
	// +kubebuilder:default=HighestOrdinal
	ScaleDownStrategy NiFiAutoscalerScaleDownStrategy `json:"scaleDownStrategy,omitempty"`
	// StabilizationSeconds is the scale-down stabilization window. A long window suits NiFi
	// because each node offload is expensive.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=300
	StabilizationSeconds int32 `json:"stabilizationSeconds,omitempty"`
	// MaxNodesPerStep bounds how many nodes a single scale-down step removes.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	MaxNodesPerStep int32 `json:"maxNodesPerStep,omitempty"`
	// ScaleDownPeriodSeconds is the period over which MaxNodesPerStep is enforced.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=300
	ScaleDownPeriodSeconds int32 `json:"scaleDownPeriodSeconds,omitempty"`
}

// NiFiAutoscalerStatus reports the rendered backend and observed scaling state.
type NiFiAutoscalerStatus struct {
	CommonStatus `json:",inline"`
	// Mode is the rendered backend: KEDA or HPA.
	Mode string `json:"mode,omitempty"`
	// ScaledObjectName is the rendered KEDA ScaledObject (empty in HPA mode).
	ScaledObjectName string `json:"scaledObjectName,omitempty"`
	// HPAName is the rendered HorizontalPodAutoscaler (empty in KEDA mode; KEDA manages its own
	// HPA internally).
	HPAName string `json:"hpaName,omitempty"`
	// CurrentReplicas is the target's current replica count.
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`
	// DesiredReplicas is the target's desired replica count.
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`
	// LastScaleTime is the last time the autoscaler scaled the target.
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.target.kind`
// +kubebuilder:printcolumn:name="Name",type=string,JSONPath=`.spec.target.name`
// +kubebuilder:printcolumn:name="Min",type=integer,JSONPath=`.spec.minReplicas`
// +kubebuilder:printcolumn:name="Max",type=integer,JSONPath=`.spec.maxReplicas`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.status.mode`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type NiFiAutoscaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NiFiAutoscalerSpec   `json:"spec,omitempty"`
	Status            NiFiAutoscalerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NiFiAutoscalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NiFiAutoscaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NiFiAutoscaler{}, &NiFiAutoscalerList{})
}
