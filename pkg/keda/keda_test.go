package keda

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
)

func TestNewScaledObjectRendersSpec(t *testing.T) {
	so, err := NewScaledObject("flow-as", "dataflows", map[string]string{"team": "data"}, ScaledObjectSpec{
		ScaleTargetRef:  ScaleTargetRef{APIVersion: "nifi.controlnifi.io/v1alpha1", Kind: "NiFiCluster", Name: "production"},
		MinReplicaCount: ptr.To(int32(3)),
		MaxReplicaCount: ptr.To(int32(9)),
		Advanced: &Advanced{HorizontalPodAutoscalerConfig: &HPAConfig{Behavior: &HPABehavior{
			ScaleDown: &HPAScalingRules{
				StabilizationWindowSeconds: ptr.To(int32(300)),
				Policies:                   []HPAScalingPolicy{{Type: "Pods", Value: 1, PeriodSeconds: 300}},
			},
		}}},
		Triggers: []Trigger{{Type: "prometheus", Name: "queue", Metadata: map[string]string{"query": "sum(nifi_amount_items_queued)"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if so.GetKind() != KindScaledObject || so.GetAPIVersion() != GroupName+"/"+Version {
		t.Fatalf("unexpected GVK: %s %s", so.GetAPIVersion(), so.GetKind())
	}
	kind, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "kind")
	if kind != "NiFiCluster" {
		t.Errorf("scaleTargetRef.kind = %q", kind)
	}
	maxReplicas, _, _ := unstructured.NestedInt64(so.Object, "spec", "maxReplicaCount")
	if maxReplicas != 9 {
		t.Errorf("maxReplicaCount = %d", maxReplicas)
	}
	triggers, _, _ := unstructured.NestedSlice(so.Object, "spec", "triggers")
	if len(triggers) != 1 {
		t.Fatalf("want 1 trigger, got %d", len(triggers))
	}
}

func TestTriggerCarriesAuthenticationRef(t *testing.T) {
	so, err := NewScaledObject("flow-as", "dataflows", nil, ScaledObjectSpec{
		ScaleTargetRef: ScaleTargetRef{Kind: "NiFiCluster", Name: "production"},
		Triggers: []Trigger{{
			Type:              "prometheus",
			Name:              "queue",
			Metadata:          map[string]string{"authModes": "bearer"},
			AuthenticationRef: &AuthenticationRef{Name: "flow-as-queue-auth"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	triggers, _, _ := unstructured.NestedSlice(so.Object, "spec", "triggers")
	trigger := triggers[0].(map[string]any)
	ref, ok := trigger["authenticationRef"].(map[string]any)
	if !ok {
		t.Fatalf("trigger missing authenticationRef: %#v", trigger)
	}
	if ref["name"] != "flow-as-queue-auth" {
		t.Errorf("authenticationRef.name = %v", ref["name"])
	}
}

func TestNewTriggerAuthenticationRendersSecretTargetRef(t *testing.T) {
	ta, err := NewTriggerAuthenticationObject("flow-as-queue-auth", "dataflows", map[string]string{"team": "data"},
		TriggerAuthenticationSpec{SecretTargetRef: []SecretTargetRef{
			{Parameter: "username", Name: "prom-creds", Key: "username"},
			{Parameter: "password", Name: "prom-creds", Key: "password"},
		}})
	if err != nil {
		t.Fatal(err)
	}
	if ta.GetKind() != KindTriggerAuthentication || ta.GetAPIVersion() != GroupName+"/"+Version {
		t.Fatalf("unexpected GVK: %s %s", ta.GetAPIVersion(), ta.GetKind())
	}
	refs, _, _ := unstructured.NestedSlice(ta.Object, "spec", "secretTargetRef")
	if len(refs) != 2 {
		t.Fatalf("want 2 secretTargetRef, got %d", len(refs))
	}
	first := refs[0].(map[string]any)
	if first["parameter"] != "username" || first["name"] != "prom-creds" || first["key"] != "username" {
		t.Errorf("unexpected secretTargetRef: %#v", first)
	}
}

func TestNewTriggerAuthenticationListGVK(t *testing.T) {
	list := NewTriggerAuthenticationList()
	if list.GetObjectKind().GroupVersionKind().Kind != KindTriggerAuthentication+"List" {
		t.Errorf("list kind = %q", list.GetObjectKind().GroupVersionKind().Kind)
	}
}

func TestIsCRDNotInstalled(t *testing.T) {
	if IsCRDNotInstalled(nil) {
		t.Error("nil must not be CRD-not-installed")
	}
	if IsCRDNotInstalled(errors.New("boom")) {
		t.Error("generic error must not be CRD-not-installed")
	}
	noMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: GroupName, Kind: KindScaledObject}}
	if !IsCRDNotInstalled(noMatch) {
		t.Error("NoKindMatchError must be CRD-not-installed")
	}
}
