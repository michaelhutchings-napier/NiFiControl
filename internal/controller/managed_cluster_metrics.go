package controller

import (
	"context"
	"fmt"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// defaultMetricsPath is NiFi 2.x's built-in Prometheus metrics endpoint, served on the
	// web port. The standalone PrometheusReportingTask was removed in NiFi 2.0.
	defaultMetricsPath = "/nifi-api/flow/metrics/prometheus"
	// managedClusterMetricsServiceLabel marks the client-facing Service as the scrape target.
	managedClusterMetricsServiceLabel = "nifi.controlnifi.io/metrics-service"
)

func metricsEnabled(cluster *nifiv1alpha1.NiFiCluster) bool {
	return cluster.Spec.Metrics != nil && cluster.Spec.Metrics.Enabled
}

func serviceMonitorEnabled(cluster *nifiv1alpha1.NiFiCluster) bool {
	return metricsEnabled(cluster) && cluster.Spec.Metrics.ServiceMonitor != nil && cluster.Spec.Metrics.ServiceMonitor.Enabled
}

func metricsPath(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.Metrics != nil && cluster.Spec.Metrics.Path != "" {
		return cluster.Spec.Metrics.Path
	}
	return defaultMetricsPath
}

func managedClusterServiceMonitorName(cluster *nifiv1alpha1.NiFiCluster) string {
	return boundedManagedName(cluster.Name, "nifi-metrics")
}

// reconcileManagedClusterMetrics renders (or removes) the Prometheus Operator ServiceMonitor
// for the managed cluster and records the metrics status. Metrics are best-effort: a missing
// Prometheus Operator installation or a transient ServiceMonitor apply error is surfaced via
// the MetricsReady condition but never marks the cluster NotReady, so it is invoked outside
// the fatal reconcile error path.
func (r *NiFiClusterReconciler) reconcileManagedClusterMetrics(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials) error {
	if !metricsEnabled(cluster) {
		return r.clearManagedClusterMetrics(ctx, cluster)
	}

	path := metricsPath(cluster)
	status := &nifiv1alpha1.NiFiClusterMetricsStatus{Enabled: true, Path: path}

	if !serviceMonitorEnabled(cluster) {
		// Metrics endpoint advertised but no ServiceMonitor requested; remove a stale one.
		if err := r.deleteManagedClusterServiceMonitor(ctx, cluster); err != nil {
			return err
		}
		return r.setManagedClusterMetricsStatus(ctx, cluster, status, metav1.ConditionTrue, "MetricsEnabled",
			fmt.Sprintf("NiFi exposes Prometheus metrics at %s; no ServiceMonitor requested.", path))
	}

	desired, err := desiredManagedClusterServiceMonitor(cluster, tls, path)
	if err != nil {
		return err
	}
	if err := r.applyServiceMonitor(ctx, cluster, desired); err != nil {
		if prometheus.IsCRDNotInstalled(err) {
			return r.setManagedClusterMetricsStatus(ctx, cluster, status, metav1.ConditionFalse, "CRDsNotInstalled",
				"Prometheus Operator CRDs (monitoring.coreos.com) are not installed; install the Prometheus Operator to scrape NiFi metrics.")
		}
		// Record the failure on the condition but do not fail the cluster reconcile.
		_ = r.setManagedClusterMetricsStatus(ctx, cluster, status, metav1.ConditionFalse, "ServiceMonitorError",
			fmt.Sprintf("Failed to apply ServiceMonitor: %v", err))
		return err
	}
	status.ServiceMonitorName = desired.GetName()
	return r.setManagedClusterMetricsStatus(ctx, cluster, status, metav1.ConditionTrue, "ServiceMonitorReady",
		fmt.Sprintf("ServiceMonitor %q scrapes NiFi metrics at %s.", desired.GetName(), path))
}

// desiredManagedClusterServiceMonitor builds the ServiceMonitor selecting the client-facing
// Service. NiFi 2.x serves metrics on the web port; on a TLS cluster the scrape uses HTTPS
// with the operator-managed client certificate (the endpoint requires authentication).
func desiredManagedClusterServiceMonitor(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials, path string) (*unstructured.Unstructured, error) {
	smSpec := cluster.Spec.Metrics.ServiceMonitor

	labels := managedClusterLabels(cluster)
	for key, value := range smSpec.Labels {
		labels[key] = value
	}

	scheme := "http"
	var tlsConfig *prometheus.TLSConfig
	if internalTLSEnabled(cluster) {
		scheme = "https"
		tlsConfig = &prometheus.TLSConfig{
			ServerName:         fmt.Sprintf("%s.%s.svc", managedClusterResourceName(cluster), cluster.Namespace),
			InsecureSkipVerify: smSpec.InsecureSkipVerify,
		}
		if !smSpec.InsecureSkipVerify && tls != nil && tls.clientSecretName != "" {
			tlsConfig.CA = &prometheus.SecretOrConfigMap{Secret: &prometheus.SecretKeySelector{Name: tls.clientSecretName, Key: "ca.crt"}}
			tlsConfig.Cert = &prometheus.SecretOrConfigMap{Secret: &prometheus.SecretKeySelector{Name: tls.clientSecretName, Key: "tls.crt"}}
			tlsConfig.KeySecret = &prometheus.SecretKeySelector{Name: tls.clientSecretName, Key: "tls.key"}
		}
	}
	// A per-endpoint TLSConfig is shared by pointer across endpoints; it is never mutated after
	// this point and is serialized independently for each endpoint.
	buildEndpoint := func(p string, params map[string][]string, interval, scrapeTimeout string) prometheus.Endpoint {
		return prometheus.Endpoint{
			Port:          "web",
			Path:          stringOrDefault(p, path),
			Scheme:        scheme,
			Interval:      stringOrDefault(interval, smSpec.Interval),
			ScrapeTimeout: stringOrDefault(scrapeTimeout, smSpec.ScrapeTimeout),
			Params:        params,
			TLSConfig:     tlsConfig,
		}
	}

	var endpoints []prometheus.Endpoint
	if len(smSpec.Endpoints) == 0 {
		endpoints = []prometheus.Endpoint{buildEndpoint("", nil, "", "")}
	} else {
		for _, e := range smSpec.Endpoints {
			endpoints = append(endpoints, buildEndpoint(e.Path, e.Params, e.Interval, e.ScrapeTimeout))
		}
	}

	spec := prometheus.ServiceMonitorSpec{
		Selector: prometheus.LabelSelector{MatchLabels: map[string]string{
			managedClusterLabel:               managedClusterResourceName(cluster),
			managedClusterMetricsServiceLabel: "true",
		}},
		NamespaceSelector: &prometheus.NamespaceSelector{MatchNames: []string{cluster.Namespace}},
		Endpoints:         endpoints,
	}
	return prometheus.NewServiceMonitor(managedClusterServiceMonitorName(cluster), cluster.Namespace, labels, spec)
}

func (r *NiFiClusterReconciler) applyServiceMonitor(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, desired *unstructured.Unstructured) error {
	existing := prometheus.New()
	existing.SetName(desired.GetName())
	existing.SetNamespace(desired.GetNamespace())
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, existing, func() error {
		existing.Object["spec"] = desired.Object["spec"]
		existing.SetLabels(desired.GetLabels())
		return controllerutil.SetControllerReference(cluster, existing, r.Scheme)
	})
	return err
}

func (r *NiFiClusterReconciler) deleteManagedClusterServiceMonitor(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	sm := prometheus.New()
	sm.SetName(managedClusterServiceMonitorName(cluster))
	sm.SetNamespace(cluster.Namespace)
	if err := r.Delete(ctx, sm); err != nil {
		if apierrors.IsNotFound(err) || prometheus.IsCRDNotInstalled(err) {
			return nil
		}
		return err
	}
	return nil
}

func (r *NiFiClusterReconciler) setManagedClusterMetricsStatus(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, status *nifiv1alpha1.NiFiClusterMetricsStatus, conditionStatus metav1.ConditionStatus, reason, message string) error {
	if metricsStatusEqual(cluster.Status.Metrics, status) && conditionMatches(cluster.Status.Conditions, nifiv1alpha1.ConditionMetricsReady, conditionStatus, reason) {
		return nil
	}
	cluster.Status.Metrics = status
	cluster.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionMetricsReady, conditionStatus, reason, message, cluster.Generation)
	return r.Status().Update(ctx, cluster)
}

// clearManagedClusterMetrics removes a previously rendered ServiceMonitor and clears the
// metrics status when metrics are disabled. Clusters that never enabled metrics keep a clean
// status (no MetricsReady condition is added).
func (r *NiFiClusterReconciler) clearManagedClusterMetrics(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	alreadyClear := cluster.Status.Metrics == nil &&
		(!conditionPresent(cluster.Status.Conditions, nifiv1alpha1.ConditionMetricsReady) ||
			conditionMatches(cluster.Status.Conditions, nifiv1alpha1.ConditionMetricsReady, metav1.ConditionFalse, "Disabled"))
	if alreadyClear {
		return nil
	}
	if err := r.deleteManagedClusterServiceMonitor(ctx, cluster); err != nil {
		return err
	}
	cluster.Status.Metrics = nil
	cluster.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionMetricsReady, metav1.ConditionFalse, "Disabled", "Metrics are disabled.", cluster.Generation)
	return r.Status().Update(ctx, cluster)
}

func metricsStatusEqual(left, right *nifiv1alpha1.NiFiClusterMetricsStatus) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func conditionPresent(conditions []metav1.Condition, conditionType nifiv1alpha1.ConditionType) bool {
	for _, condition := range conditions {
		if condition.Type == string(conditionType) {
			return true
		}
	}
	return false
}
