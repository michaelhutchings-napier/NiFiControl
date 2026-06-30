package prometheus

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestNewServiceMonitorRendersSpec(t *testing.T) {
	sm, err := NewServiceMonitor("nifi-metrics", "dataflows", map[string]string{"team": "data"}, ServiceMonitorSpec{
		Selector:          LabelSelector{MatchLabels: map[string]string{"app": "nifi"}},
		NamespaceSelector: &NamespaceSelector{MatchNames: []string{"dataflows"}},
		Endpoints: []Endpoint{{
			Port:   "web",
			Path:   "/nifi-api/flow/metrics/prometheus",
			Scheme: "https",
			TLSConfig: &TLSConfig{
				ServerName: "nifi.dataflows.svc",
				CA:         &SecretOrConfigMap{Secret: &SecretKeySelector{Name: "tls", Key: "ca.crt"}},
				KeySecret:  &SecretKeySelector{Name: "tls", Key: "tls.key"},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sm.GetKind() != KindServiceMonitor || sm.GetAPIVersion() != GroupName+"/"+Version {
		t.Fatalf("unexpected GVK: %s %s", sm.GetAPIVersion(), sm.GetKind())
	}
	if sm.GetLabels()["team"] != "data" {
		t.Errorf("labels not set: %v", sm.GetLabels())
	}
	endpoints, found, err := unstructured.NestedSlice(sm.Object, "spec", "endpoints")
	if err != nil || !found || len(endpoints) != 1 {
		t.Fatalf("endpoints not rendered (found=%v err=%v)", found, err)
	}
	endpoint := endpoints[0].(map[string]any)
	if endpoint["scheme"] != "https" || endpoint["port"] != "web" {
		t.Errorf("endpoint not rendered correctly: %v", endpoint)
	}
	serverName, _, _ := unstructured.NestedString(endpoint, "tlsConfig", "serverName")
	if serverName != "nifi.dataflows.svc" {
		t.Errorf("tlsConfig.serverName = %q", serverName)
	}
}

func TestIsCRDNotInstalled(t *testing.T) {
	if IsCRDNotInstalled(nil) {
		t.Error("nil error must not be classified as CRD-not-installed")
	}
	if IsCRDNotInstalled(errors.New("connection refused")) {
		t.Error("a generic error must not be classified as CRD-not-installed")
	}
	noMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: GroupName, Kind: KindServiceMonitor}}
	if !IsCRDNotInstalled(noMatch) {
		t.Error("a NoKindMatchError must be classified as CRD-not-installed")
	}
}
