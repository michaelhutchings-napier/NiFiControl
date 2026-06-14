package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCommonStatusMarkAccepted(t *testing.T) {
	status := CommonStatus{}

	status.MarkAccepted(7)

	if status.ObservedGeneration != 7 {
		t.Fatalf("observedGeneration = %d, want 7", status.ObservedGeneration)
	}
	if status.Ready {
		t.Fatal("ready = true, want false while NiFi reconciliation is not implemented")
	}
	assertCondition(t, status.Conditions, ConditionReady, metav1.ConditionFalse, "ReconciliationPending")
	assertCondition(t, status.Conditions, ConditionReconciling, metav1.ConditionTrue, "Accepted")
}

func TestSetConditionPreservesTransitionTimeWhenStatusDoesNotChange(t *testing.T) {
	status := CommonStatus{}
	status.SetCondition(ConditionReady, metav1.ConditionFalse, "First", "first message", 1)

	firstTransition := status.Conditions[0].LastTransitionTime
	status.SetCondition(ConditionReady, metav1.ConditionFalse, "Second", "second message", 2)

	if len(status.Conditions) != 1 {
		t.Fatalf("conditions length = %d, want 1", len(status.Conditions))
	}
	if !status.Conditions[0].LastTransitionTime.Equal(&firstTransition) {
		t.Fatal("lastTransitionTime changed even though condition status did not change")
	}
	if status.Conditions[0].Reason != "Second" {
		t.Fatalf("reason = %q, want Second", status.Conditions[0].Reason)
	}
}

func assertCondition(t *testing.T, conditions []metav1.Condition, conditionType ConditionType, conditionStatus metav1.ConditionStatus, reason string) {
	t.Helper()
	for _, condition := range conditions {
		if condition.Type != string(conditionType) {
			continue
		}
		if condition.Status != conditionStatus {
			t.Fatalf("%s status = %s, want %s", conditionType, condition.Status, conditionStatus)
		}
		if condition.Reason != reason {
			t.Fatalf("%s reason = %q, want %q", conditionType, condition.Reason, reason)
		}
		return
	}
	t.Fatalf("condition %s not found", conditionType)
}
