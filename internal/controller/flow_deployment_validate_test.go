package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func validateDeployment() *nifiv1alpha1.NiFiFlowDeployment {
	return &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "flowtest", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef:   nifiv1alpha1.ClusterReference{Name: "production"},
			ValidateOnly: true,
		},
	}
}

var validateSnapshot = json.RawMessage(`{"flowContents":{"name":"flowtest"}}`)

// reloadDeployment fetches the persisted deployment so each reconcile step observes the status
// exactly as a real controller would across requeues.
func reloadDeployment(t *testing.T, c client.Client, name string) *nifiv1alpha1.NiFiFlowDeployment {
	t.Helper()
	out := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, out); err != nil {
		t.Fatal(err)
	}
	return out
}

func runValidation(t *testing.T, r *NiFiFlowDeploymentReconciler, k8sClient client.Client, deployment *nifiv1alpha1.NiFiFlowDeployment) *nifiv1alpha1.NiFiFlowDeployment {
	t.Helper()
	current := deployment
	// Import -> Validating.
	if _, err := r.reconcileFlowValidation(context.Background(), current, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	current = reloadDeployment(t, k8sClient, deployment.Name)
	if current.Status.ValidationPhase != flowValidationPhaseValidating || current.Status.ValidationProcessGroupID != "vpg" {
		t.Fatalf("after import: phase=%q pg=%q", current.Status.ValidationPhase, current.Status.ValidationProcessGroupID)
	}
	// Inspect -> Cleanup.
	if _, err := r.reconcileFlowValidation(context.Background(), current, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	current = reloadDeployment(t, k8sClient, deployment.Name)
	if current.Status.ValidationPhase != flowValidationPhaseCleanup || current.Status.ValidationResult == nil {
		t.Fatalf("after inspect: phase=%q result=%v", current.Status.ValidationPhase, current.Status.ValidationResult)
	}
	// Cleanup -> terminal.
	if _, err := r.reconcileFlowValidation(context.Background(), current, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	return reloadDeployment(t, k8sClient, deployment.Name)
}

func TestFlowValidationValidFlow(t *testing.T) {
	deployment := validateDeployment()
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	nf.candidate = &fakeCandidate{id: "vpg"}
	nf.validationReports["vpg"] = nifi.FlowValidationReport{Total: 3}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf, FlowValidationClient: nf}

	final := runValidation(t, r, k8sClient, deployment)
	if !final.Status.Ready {
		t.Fatalf("valid flow should be Ready, syncState=%q", final.Status.SyncState)
	}
	if final.Status.ValidationResult == nil || !final.Status.ValidationResult.Valid {
		t.Fatalf("validationResult should be valid, got %#v", final.Status.ValidationResult)
	}
	if final.Status.ValidationProcessGroupID != "" || final.Status.ValidationPhase != "" {
		t.Fatalf("temporary state should be cleared, got pg=%q phase=%q", final.Status.ValidationProcessGroupID, final.Status.ValidationPhase)
	}
	if pg := nf.pgs["vpg"]; pg == nil || !pg.deleted {
		t.Fatalf("temporary process group should be deleted, got %#v", pg)
	}
	if nf.csState["vpg"] != nifi.RunStateDisabled {
		t.Fatalf("controller services should be disabled before deletion, got %q", nf.csState["vpg"])
	}

	// Churn guard: re-running the same content at the same generation must not re-import.
	before := nf.imports
	if _, err := r.reconcileFlowValidation(context.Background(), final, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	if nf.imports != before {
		t.Fatalf("already-validated content should not re-import: imports %d -> %d", before, nf.imports)
	}
}

func TestFlowValidationInvalidFlow(t *testing.T) {
	deployment := validateDeployment()
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	nf.candidate = &fakeCandidate{id: "vpg"}
	nf.validationReports["vpg"] = nifi.FlowValidationReport{
		Total: 2,
		Invalid: []nifi.InvalidComponent{
			{Kind: nifi.ValidationKindProcessor, ID: "p1", Name: "Broken", Type: "org.apache.nifi.X", ProcessGroupID: "vpg", ValidationErrors: []string{"'Prop' is invalid"}},
		},
	}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf, FlowValidationClient: nf}

	final := runValidation(t, r, k8sClient, deployment)
	if final.Status.Ready {
		t.Fatal("invalid flow must not be Ready")
	}
	assertControllerCondition(t, final.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "ValidationFailed")
	result := final.Status.ValidationResult
	if result == nil || result.Valid || result.InvalidCount != 1 {
		t.Fatalf("validationResult = %#v", result)
	}
	if len(result.InvalidComponents) != 1 || result.InvalidComponents[0].Name != "Broken" || len(result.InvalidComponents[0].Errors) != 1 {
		t.Fatalf("invalid components = %#v", result.InvalidComponents)
	}
	// The temporary group is still cleaned up even when the flow is invalid.
	if pg := nf.pgs["vpg"]; pg == nil || !pg.deleted {
		t.Fatalf("temporary process group should be deleted on invalid flow, got %#v", pg)
	}
}

func TestFlowValidationWaitsForComponentsToSettle(t *testing.T) {
	deployment := validateDeployment()
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	nf.candidate = &fakeCandidate{id: "vpg"}
	nf.validationReports["vpg"] = nifi.FlowValidationReport{Total: 2, ValidatingCount: 1}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf, FlowValidationClient: nf}

	// Import.
	if _, err := r.reconcileFlowValidation(context.Background(), deployment, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	current := reloadDeployment(t, k8sClient, deployment.Name)
	// Inspect while a component is still validating -> stays in Validating, no result yet.
	if _, err := r.reconcileFlowValidation(context.Background(), current, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	current = reloadDeployment(t, k8sClient, deployment.Name)
	if current.Status.ValidationPhase != flowValidationPhaseValidating || current.Status.ValidationResult != nil {
		t.Fatalf("should still be waiting: phase=%q result=%v", current.Status.ValidationPhase, current.Status.ValidationResult)
	}

	// Force the settle timeout to expire: the run finalizes as inconclusive (not valid).
	past := metav1.NewTime(time.Now().Add(-flowValidationSettleTimeout - time.Minute))
	current.Status.ValidationStartedAt = &past
	if err := k8sClient.Status().Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileFlowValidation(context.Background(), current, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	current = reloadDeployment(t, k8sClient, deployment.Name)
	if current.Status.ValidationResult == nil || current.Status.ValidationResult.Valid {
		t.Fatalf("settle timeout with a validating component should be inconclusive (not valid), got %#v", current.Status.ValidationResult)
	}
}

func TestFlowValidationRefusesLiveDeployment(t *testing.T) {
	deployment := validateDeployment()
	deployment.Status.ProcessGroupID = "live-pg"
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf, FlowValidationClient: nf}

	if _, err := r.reconcileFlowValidation(context.Background(), deployment, "https://nifi", "root", validateSnapshot, "v1", "sha256:v1"); err != nil {
		t.Fatal(err)
	}
	current := reloadDeployment(t, k8sClient, deployment.Name)
	if current.Status.Ready {
		t.Fatal("validateOnly on a live deployment must not be Ready")
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "ValidateOnlyConflict")
	if nf.imports != 0 {
		t.Fatalf("must not import over a live deployment, imports=%d", nf.imports)
	}
}

func TestFlowSnapshotWithName(t *testing.T) {
	out, err := flowSnapshotWithName(json.RawMessage(`{"flowContents":{"name":"original","x":1}}`), "renamed")
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		FlowContents struct {
			Name string `json:"name"`
			X    int    `json:"x"`
		} `json:"flowContents"`
	}
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.FlowContents.Name != "renamed" || decoded.FlowContents.X != 1 {
		t.Fatalf("rename result = %s", out)
	}
}
