package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/keda"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiautoscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiautoscalers/finalizers,verbs=update
// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=keda.sh,resources=triggerauthentications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete

const (
	autoscalerModeKEDA = "KEDA"
	autoscalerModeHPA  = "HPA"
)

// NiFiAutoscalerReconciler renders a KEDA ScaledObject (Prometheus/external metrics) or a
// native HorizontalPodAutoscaler (CPU/memory) that drives a NiFiCluster or NiFiNodeGroup
// scale subresource. It owns the rendered object (cleaned up by ownerReference GC), applies
// NiFi-safe defaults, and reports a single status surface.
type NiFiAutoscalerReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

func (r *NiFiAutoscalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiAutoscaler{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	// Owned ScaledObject/HPA are cleaned up by ownerReference garbage collection, so no
	// finalizer is needed.
	if !instance.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Resolve the scale target.
	current, desired, managed, found, err := r.resolveTarget(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !found {
		return r.notReady(ctx, instance, "TargetNotFound",
			fmt.Sprintf("%s %q was not found in namespace %q.", instance.Spec.Target.Kind, instance.Spec.Target.Name, instance.Namespace))
	}
	if !managed {
		return r.notReady(ctx, instance, "TargetNotScalable",
			"The target NiFiCluster must be operator-managed (mode Internal) to be autoscaled.")
	}

	mode, reason, message := autoscalerMode(instance)
	if mode == "" {
		return r.notReady(ctx, instance, reason, message)
	}

	switch mode {
	case autoscalerModeKEDA:
		// Remove a stale HPA from a previous Resource-metric configuration.
		if err := r.deleteOwnedHPA(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
		so, auths, err := desiredAutoscalerObjects(instance)
		if err != nil {
			return ctrl.Result{}, err
		}
		// Apply the TriggerAuthentications before the ScaledObject that references them.
		for _, ta := range auths {
			if err := r.applyOwnedObject(ctx, instance, ta, keda.NewTriggerAuthentication()); err != nil {
				if keda.IsCRDNotInstalled(err) {
					return r.notReady(ctx, instance, "KEDANotInstalled",
						"KEDA CRDs (keda.sh) are not installed; install KEDA to autoscale on Prometheus/external metrics.")
				}
				return ctrl.Result{}, err
			}
		}
		if err := r.applyOwnedObject(ctx, instance, so, keda.New()); err != nil {
			if keda.IsCRDNotInstalled(err) {
				return r.notReady(ctx, instance, "KEDANotInstalled",
					"KEDA CRDs (keda.sh) are not installed; install KEDA to autoscale on Prometheus/external metrics.")
			}
			return ctrl.Result{}, err
		}
		// Prune TriggerAuthentications no longer referenced (auth removed or a trigger renamed).
		keep := make(map[string]bool, len(auths))
		for _, ta := range auths {
			keep[ta.GetName()] = true
		}
		if err := r.pruneOwnedTriggerAuths(ctx, instance, keep); err != nil {
			return ctrl.Result{}, err
		}
		return r.ready(ctx, instance, autoscalerModeKEDA, so.GetName(), "", current, desired)
	case autoscalerModeHPA:
		// Remove a stale ScaledObject and any TriggerAuthentications from a previous Prometheus
		// configuration (tolerate KEDA absent).
		if err := r.deleteOwnedScaledObject(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.pruneOwnedTriggerAuths(ctx, instance, nil); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.applyHPA(ctx, instance); err != nil {
			return ctrl.Result{}, err
		}
		return r.ready(ctx, instance, autoscalerModeHPA, "", autoscalerResourceName(instance), current, desired)
	}
	return ctrl.Result{}, nil
}

// resolveTarget returns the target's current/desired replicas, whether it is operator-managed
// (scalable), and whether it exists.
func (r *NiFiAutoscalerReconciler) resolveTarget(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler) (current, desired int32, managed, found bool, err error) {
	key := types.NamespacedName{Name: instance.Spec.Target.Name, Namespace: instance.Namespace}
	switch instance.Spec.Target.Kind {
	case "NiFiNodeGroup":
		group := &nifiv1alpha1.NiFiNodeGroup{}
		if err := r.Get(ctx, key, group); err != nil {
			if apierrors.IsNotFound(err) {
				return 0, 0, false, false, nil
			}
			return 0, 0, false, false, err
		}
		return group.Status.Replicas, group.Spec.Replicas, true, true, nil
	default: // NiFiCluster
		cluster := &nifiv1alpha1.NiFiCluster{}
		if err := r.Get(ctx, key, cluster); err != nil {
			if apierrors.IsNotFound(err) {
				return 0, 0, false, false, nil
			}
			return 0, 0, false, false, err
		}
		return cluster.Status.Replicas, managedClusterReplicas(cluster), resolvedClusterMode(cluster) == nifiv1alpha1.ClusterModeInternal, true, nil
	}
}

// autoscalerMode determines the rendering backend from the metric types, or returns a reason
// when the metric set is invalid.
func autoscalerMode(instance *nifiv1alpha1.NiFiAutoscaler) (mode, reason, message string) {
	var prometheus, resource int
	for _, metric := range instance.Spec.Metrics {
		switch metric.Type {
		case "Prometheus":
			prometheus++
		case "Resource":
			resource++
		}
	}
	switch {
	case prometheus > 0 && resource > 0:
		return "", "MixedMetricTypes", "Mix Prometheus and Resource metrics is not supported; use one family per NiFiAutoscaler."
	case prometheus > 0:
		return autoscalerModeKEDA, "", ""
	case resource > 0:
		return autoscalerModeHPA, "", ""
	default:
		return "", "NoMetrics", "At least one Prometheus or Resource metric is required."
	}
}

func autoscalerResourceName(instance *nifiv1alpha1.NiFiAutoscaler) string {
	return boundedManagedName(instance.Name, "nifiautoscaler")
}

func autoscalerMinReplicas(instance *nifiv1alpha1.NiFiAutoscaler) int32 {
	if instance.Spec.MinReplicas > 0 {
		return instance.Spec.MinReplicas
	}
	return 1
}

// behaviorDefaults resolves the NiFi-safe scale-down behavior, filling defaults.
func behaviorDefaults(instance *nifiv1alpha1.NiFiAutoscaler) (stabilization, maxNodes, period int32) {
	stabilization, maxNodes, period = 300, 1, 300
	if b := instance.Spec.Behavior; b != nil {
		if b.StabilizationSeconds >= 0 && (b.StabilizationSeconds != 0 || b.MaxNodesPerStep != 0) {
			stabilization = b.StabilizationSeconds
		}
		if b.MaxNodesPerStep > 0 {
			maxNodes = b.MaxNodesPerStep
		}
		if b.ScaleDownPeriodSeconds > 0 {
			period = b.ScaleDownPeriodSeconds
		}
	}
	return stabilization, maxNodes, period
}

func autoscalerTargetAPIVersion() string {
	return nifiv1alpha1.GroupVersion.Group + "/" + nifiv1alpha1.GroupVersion.Version
}

func autoscalerLabels(instance *nifiv1alpha1.NiFiAutoscaler) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by":       "nificontrol",
		"app.kubernetes.io/component":        "autoscaler",
		"nifi.controlnifi.io/nifiautoscaler": instance.Name,
	}
}

// autoscalerAuthName is the deterministic name of the TriggerAuthentication rendered for one
// authenticated Prometheus trigger.
func autoscalerAuthName(instance *nifiv1alpha1.NiFiAutoscaler, triggerName string) string {
	return boundedManagedName(instance.Name+"-"+triggerName, "auth")
}

// prometheusAuthParams maps a PrometheusAuthentication to the KEDA authModes value and the
// TriggerAuthentication secretTargetRef entries for that mode. Empty key overrides fall back to
// the conventional Secret keys so the mapping is robust without CRD defaulting (e.g. in tests).
func prometheusAuthParams(a *nifiv1alpha1.PrometheusAuthentication) (mode string, refs []keda.SecretTargetRef) {
	orDefault := func(v, d string) string {
		if v == "" {
			return d
		}
		return v
	}
	switch a.Mode {
	case "Bearer":
		return "bearer", []keda.SecretTargetRef{
			{Parameter: "bearerToken", Name: a.SecretName, Key: orDefault(a.BearerTokenKey, "bearerToken")},
		}
	case "Basic":
		return "basic", []keda.SecretTargetRef{
			{Parameter: "username", Name: a.SecretName, Key: orDefault(a.UsernameKey, "username")},
			{Parameter: "password", Name: a.SecretName, Key: orDefault(a.PasswordKey, "password")},
		}
	case "TLS":
		return "tls", []keda.SecretTargetRef{
			{Parameter: "ca", Name: a.SecretName, Key: orDefault(a.CAKey, "ca.crt")},
			{Parameter: "cert", Name: a.SecretName, Key: orDefault(a.CertKey, "tls.crt")},
			{Parameter: "key", Name: a.SecretName, Key: orDefault(a.KeyKey, "tls.key")},
		}
	}
	return "", nil
}

// desiredAutoscalerObjects renders the KEDA ScaledObject and any TriggerAuthentications its
// Prometheus triggers need. Each authenticated trigger gets its own TriggerAuthentication.
func desiredAutoscalerObjects(instance *nifiv1alpha1.NiFiAutoscaler) (*unstructured.Unstructured, []*unstructured.Unstructured, error) {
	stabilization, maxNodes, period := behaviorDefaults(instance)

	triggers := make([]keda.Trigger, 0, len(instance.Spec.Metrics))
	var auths []*unstructured.Unstructured
	for i, metric := range instance.Spec.Metrics {
		source := metric.Prometheus
		name := source.Name
		if name == "" {
			name = fmt.Sprintf("prometheus-%d", i)
		}
		metadata := map[string]string{
			"serverAddress": source.ServerAddress,
			"query":         source.Query,
			"threshold":     source.Threshold,
		}
		if source.UnsafeSSL {
			metadata["unsafeSsl"] = "true"
		}
		trigger := keda.Trigger{Type: "prometheus", Name: name, Metadata: metadata}
		if source.Authentication != nil {
			authName := autoscalerAuthName(instance, name)
			authModes, refs := prometheusAuthParams(source.Authentication)
			metadata["authModes"] = authModes
			trigger.AuthenticationRef = &keda.AuthenticationRef{Name: authName}
			ta, err := keda.NewTriggerAuthenticationObject(authName, instance.Namespace, autoscalerLabels(instance),
				keda.TriggerAuthenticationSpec{SecretTargetRef: refs})
			if err != nil {
				return nil, nil, err
			}
			auths = append(auths, ta)
		}
		triggers = append(triggers, trigger)
	}

	spec := keda.ScaledObjectSpec{
		ScaleTargetRef: keda.ScaleTargetRef{
			APIVersion: autoscalerTargetAPIVersion(),
			Kind:       instance.Spec.Target.Kind,
			Name:       instance.Spec.Target.Name,
		},
		MinReplicaCount: ptr.To(autoscalerMinReplicas(instance)),
		MaxReplicaCount: ptr.To(instance.Spec.MaxReplicas),
		PollingInterval: instance.Spec.PollingInterval,
		Advanced: &keda.Advanced{
			HorizontalPodAutoscalerConfig: &keda.HPAConfig{
				Behavior: &keda.HPABehavior{
					ScaleDown: &keda.HPAScalingRules{
						StabilizationWindowSeconds: ptr.To(stabilization),
						Policies: []keda.HPAScalingPolicy{
							{Type: "Pods", Value: maxNodes, PeriodSeconds: period},
						},
					},
				},
			},
		},
		Triggers: triggers,
	}
	so, err := keda.NewScaledObject(autoscalerResourceName(instance), instance.Namespace, autoscalerLabels(instance), spec)
	if err != nil {
		return nil, nil, err
	}
	return so, auths, nil
}

// applyOwnedObject creates or updates an owned unstructured KEDA object (ScaledObject or
// TriggerAuthentication). existing is an empty object of the desired kind used as the mutation
// target.
func (r *NiFiAutoscalerReconciler) applyOwnedObject(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler, desired, existing *unstructured.Unstructured) error {
	existing.SetName(desired.GetName())
	existing.SetNamespace(desired.GetNamespace())
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, existing, func() error {
		existing.Object["spec"] = desired.Object["spec"]
		existing.SetLabels(desired.GetLabels())
		return controllerutil.SetControllerReference(instance, existing, r.Scheme)
	})
	return err
}

// pruneOwnedTriggerAuths deletes owned TriggerAuthentications whose names are not in keep (pass a
// nil/empty keep to delete all). Tolerates KEDA being absent.
func (r *NiFiAutoscalerReconciler) pruneOwnedTriggerAuths(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler, keep map[string]bool) error {
	list := keda.NewTriggerAuthenticationList()
	if err := r.List(ctx, list,
		client.InNamespace(instance.Namespace),
		client.MatchingLabels{"nifi.controlnifi.io/nifiautoscaler": instance.Name}); err != nil {
		if keda.IsCRDNotInstalled(err) {
			return nil
		}
		return err
	}
	for i := range list.Items {
		item := &list.Items[i]
		if keep[item.GetName()] {
			continue
		}
		if err := r.Delete(ctx, item); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *NiFiAutoscalerReconciler) applyHPA(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler) error {
	stabilization, maxNodes, period := behaviorDefaults(instance)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: autoscalerResourceName(instance), Namespace: instance.Namespace},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		hpa.Labels = autoscalerLabels(instance)
		hpa.Spec.ScaleTargetRef = autoscalingv2.CrossVersionObjectReference{
			APIVersion: autoscalerTargetAPIVersion(),
			Kind:       instance.Spec.Target.Kind,
			Name:       instance.Spec.Target.Name,
		}
		hpa.Spec.MinReplicas = ptr.To(autoscalerMinReplicas(instance))
		hpa.Spec.MaxReplicas = instance.Spec.MaxReplicas

		metrics := make([]autoscalingv2.MetricSpec, 0, len(instance.Spec.Metrics))
		for _, metric := range instance.Spec.Metrics {
			metrics = append(metrics, autoscalingv2.MetricSpec{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: corev1.ResourceName(metric.Resource.Name),
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: ptr.To(metric.Resource.TargetAverageUtilization),
					},
				},
			})
		}
		hpa.Spec.Metrics = metrics
		hpa.Spec.Behavior = &autoscalingv2.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscalingv2.HPAScalingRules{
				StabilizationWindowSeconds: ptr.To(stabilization),
				Policies: []autoscalingv2.HPAScalingPolicy{
					{Type: autoscalingv2.PodsScalingPolicy, Value: maxNodes, PeriodSeconds: period},
				},
			},
		}
		return controllerutil.SetControllerReference(instance, hpa, r.Scheme)
	})
	return err
}

func (r *NiFiAutoscalerReconciler) deleteOwnedScaledObject(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler) error {
	so := keda.New()
	so.SetName(autoscalerResourceName(instance))
	so.SetNamespace(instance.Namespace)
	if err := r.Delete(ctx, so); err != nil {
		if apierrors.IsNotFound(err) || keda.IsCRDNotInstalled(err) {
			return nil
		}
		return err
	}
	return nil
}

func (r *NiFiAutoscalerReconciler) deleteOwnedHPA(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: autoscalerResourceName(instance), Namespace: instance.Namespace},
	}
	if err := r.Delete(ctx, hpa); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *NiFiAutoscalerReconciler) ready(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler, mode, scaledObjectName, hpaName string, current, desired int32) (ctrl.Result, error) {
	message := fmt.Sprintf("Autoscaling %s/%s via %s (%d-%d replicas).", instance.Spec.Target.Kind, instance.Spec.Target.Name, mode, autoscalerMinReplicas(instance), instance.Spec.MaxReplicas)
	if autoscalerStatusMatches(instance, true, "AutoscalerReady", mode, scaledObjectName, hpaName, current, desired) {
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}
	instance.Status.CommonStatus.MarkReady(instance.Generation, "AutoscalerReady", message)
	instance.Status.Mode = mode
	instance.Status.ScaledObjectName = scaledObjectName
	instance.Status.HPAName = hpaName
	instance.Status.CurrentReplicas = current
	instance.Status.DesiredReplicas = desired
	instance.Status.Sync.LastError = ""
	if err := r.Status().Update(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}
	recordEvent(r.Recorder, instance, "Normal", "AutoscalerReady", message)
	// Requeue to keep current/desired replica status fresh.
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *NiFiAutoscalerReconciler) notReady(ctx context.Context, instance *nifiv1alpha1.NiFiAutoscaler, reason, message string) (ctrl.Result, error) {
	if autoscalerStatusMatches(instance, false, reason, instance.Status.Mode, instance.Status.ScaledObjectName, instance.Status.HPAName, instance.Status.CurrentReplicas, instance.Status.DesiredReplicas) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	instance.Status.CommonStatus.MarkNotReady(instance.Generation, reason, message)
	instance.Status.Sync.LastError = message
	if err := r.Status().Update(ctx, instance); err != nil {
		return ctrl.Result{}, err
	}
	recordEvent(r.Recorder, instance, "Warning", reason, message)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func autoscalerStatusMatches(instance *nifiv1alpha1.NiFiAutoscaler, ready bool, reason, mode, scaledObjectName, hpaName string, current, desired int32) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready != ready {
		return false
	}
	if instance.Status.Mode != mode || instance.Status.ScaledObjectName != scaledObjectName || instance.Status.HPAName != hpaName {
		return false
	}
	if instance.Status.CurrentReplicas != current || instance.Status.DesiredReplicas != desired {
		return false
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason == reason
		}
	}
	return false
}

func (r *NiFiAutoscalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiAutoscaler{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForTarget("NiFiCluster"))).
		Watches(&nifiv1alpha1.NiFiNodeGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForTarget("NiFiNodeGroup"))).
		Complete(r)
}

// requestsForTarget enqueues autoscalers whose target matches the changed resource, so the
// autoscaler status tracks the target's current/desired replicas.
func (r *NiFiAutoscalerReconciler) requestsForTarget(kind string) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		list := &nifiv1alpha1.NiFiAutoscalerList{}
		if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
			return nil
		}
		var requests []reconcile.Request
		for i := range list.Items {
			autoscaler := &list.Items[i]
			targetKind := autoscaler.Spec.Target.Kind
			if targetKind == "" {
				targetKind = "NiFiCluster"
			}
			if targetKind == kind && autoscaler.Spec.Target.Name == obj.GetName() {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: autoscaler.Name, Namespace: autoscaler.Namespace}})
			}
		}
		return requests
	}
}
