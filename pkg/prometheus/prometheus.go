// Package prometheus provides a minimal, dependency-free typed surface over the
// monitoring.coreos.com/v1 ServiceMonitor resource (Prometheus Operator). NiFiControl
// deliberately avoids importing the upstream prometheus-operator Go module because its
// pinned k8s.io versions conflict with this project's. Instead, small typed structs are
// converted to *unstructured.Unstructured for apply, which also lets the operator detect a
// missing Prometheus Operator installation gracefully (no REST mapping) rather than
// crashing. This mirrors the approach in the certmanager package.
package prometheus

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
	// GroupName is the Prometheus Operator API group.
	GroupName = "monitoring.coreos.com"
	// Version is the Prometheus Operator API version NiFiControl targets.
	Version = "v1"

	KindServiceMonitor = "ServiceMonitor"
)

// GroupVersion identifies the Prometheus Operator API surface used by NiFiControl.
var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// ServiceMonitorGVK is the GroupVersionKind of the ServiceMonitor resource.
var ServiceMonitorGVK = GroupVersion.WithKind(KindServiceMonitor)

// LabelSelector selects the Service(s) a ServiceMonitor scrapes.
type LabelSelector struct {
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

// NamespaceSelector bounds which namespaces a ServiceMonitor selects Services from.
type NamespaceSelector struct {
	MatchNames []string `json:"matchNames,omitempty"`
}

// SecretKeySelector references a key within a Secret.
type SecretKeySelector struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// SecretOrConfigMap selects a value from either a Secret or a ConfigMap. NiFiControl only
// uses the Secret form.
type SecretOrConfigMap struct {
	Secret *SecretKeySelector `json:"secret,omitempty"`
}

// TLSConfig configures TLS for a scrape endpoint.
type TLSConfig struct {
	CA                 *SecretOrConfigMap `json:"ca,omitempty"`
	Cert               *SecretOrConfigMap `json:"cert,omitempty"`
	KeySecret          *SecretKeySelector `json:"keySecret,omitempty"`
	ServerName         string             `json:"serverName,omitempty"`
	InsecureSkipVerify bool               `json:"insecureSkipVerify,omitempty"`
}

// Endpoint describes a single scrape target on the selected Service.
type Endpoint struct {
	Port          string              `json:"port,omitempty"`
	Path          string              `json:"path,omitempty"`
	Scheme        string              `json:"scheme,omitempty"`
	Interval      string              `json:"interval,omitempty"`
	ScrapeTimeout string              `json:"scrapeTimeout,omitempty"`
	Params        map[string][]string `json:"params,omitempty"`
	TLSConfig     *TLSConfig          `json:"tlsConfig,omitempty"`
}

// ServiceMonitorSpec is the subset of the Prometheus Operator ServiceMonitor spec that
// NiFiControl sets.
type ServiceMonitorSpec struct {
	Selector          LabelSelector      `json:"selector"`
	NamespaceSelector *NamespaceSelector `json:"namespaceSelector,omitempty"`
	Endpoints         []Endpoint         `json:"endpoints"`
}

// New returns an empty unstructured ServiceMonitor, suitable as a target for client.Get or
// controllerutil.CreateOrUpdate.
func New() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(ServiceMonitorGVK)
	return obj
}

// NewServiceMonitor builds an unstructured namespaced ServiceMonitor with the given labels
// and spec.
func NewServiceMonitor(name, namespace string, labels map[string]string, spec ServiceMonitorSpec) (*unstructured.Unstructured, error) {
	specMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&spec)
	if err != nil {
		return nil, fmt.Errorf("convert ServiceMonitor spec: %w", err)
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
		return nil, fmt.Errorf("set ServiceMonitor spec: %w", err)
	}
	return obj, nil
}

// IsCRDNotInstalled reports whether err indicates the Prometheus Operator CRDs are absent
// from the cluster (no REST mapping for the ServiceMonitor kind). The operator surfaces
// this as a non-fatal status rather than treating it as a transient failure.
func IsCRDNotInstalled(err error) bool {
	if err == nil {
		return false
	}
	if meta.IsNoMatchError(err) {
		return true
	}
	// A scheme-backed client (notably the fake client used in tests) reports an
	// unregistered kind rather than a REST-mapping miss; treat it the same way.
	if runtime.IsNotRegisteredError(err) {
		return true
	}
	// The discovery client reports a missing resource type as a NotFound on the
	// GroupResource; an object NotFound carries a resource name, which lets us tell the two
	// apart.
	if apierrors.IsNotFound(err) {
		var statusErr *apierrors.StatusError
		if errors.As(err, &statusErr) {
			details := statusErr.ErrStatus.Details
			return details != nil && details.Name == "" && details.Kind != ""
		}
	}
	return false
}
