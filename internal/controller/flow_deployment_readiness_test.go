package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func boolPtr(v bool) *bool { return &v }

func readinessDeployment(readiness *nifiv1alpha1.RolloutReadiness) *nifiv1alpha1.NiFiFlowDeployment {
	ignore := nifiv1alpha1.DriftPolicy{Mode: nifiv1alpha1.DriftPolicyIgnore}
	return &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 3},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: "production"},
			Target:      nifiv1alpha1.FlowDeploymentTarget{ProcessGroupName: "payments"},
			Rollout:     nifiv1alpha1.RolloutStrategy{Strategy: "ApplyOnly", Readiness: readiness},
			DriftPolicy: ignore,
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			ProcessGroupID: "pg",
			ActiveRollout:  &nifiv1alpha1.FlowRolloutStatus{Operation: "Rollout", Strategy: "ApplyOnly", Phase: bgPhaseAwaitingReadiness, TargetVersion: "v2", TargetDigest: "sha256:new", StartedAt: metav1.Now()},
		},
	}
}

func readinessClient(t *testing.T, deployment *nifiv1alpha1.NiFiFlowDeployment) (client.Client, *runtime.Scheme) {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).WithStatusSubresource(&nifiv1alpha1.NiFiFlowDeployment{}).Build()
	return c, scheme
}

func TestRolloutReadinessGateWaitsThenSucceeds(t *testing.T) {
	deployment := readinessDeployment(&nifiv1alpha1.RolloutReadiness{RequireValidComponents: boolPtr(true), TimeoutSeconds: 300})
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	nf.pgs["pg"] = &fakePG{id: "pg", name: "payments", invalidCount: 2}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf}
	snapshot := json.RawMessage(`{"flowContents":{"name":"payments"}}`)

	// Invalid components -> not ready yet.
	if _, err := r.reconcileRolloutReadiness(context.Background(), deployment, "https://nifi", "v2", "sha256:new", snapshot); err != nil {
		t.Fatal(err)
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: "payments", Namespace: "default"}, current)
	if current.Status.SyncState != "AwaitingReadiness" || current.Status.ActiveRollout == nil {
		t.Fatalf("expected AwaitingReadiness wait, got syncState=%q rollout=%v", current.Status.SyncState, current.Status.ActiveRollout)
	}

	// Components become valid -> rollout completes.
	nf.pgs["pg"].invalidCount = 0
	if _, err := r.reconcileRolloutReadiness(context.Background(), current, "https://nifi", "v2", "sha256:new", snapshot); err != nil {
		t.Fatal(err)
	}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: "payments", Namespace: "default"}, current)
	if !current.Status.Ready {
		t.Fatalf("deployment should be Ready after readiness gate cleared, syncState=%q", current.Status.SyncState)
	}
	if current.Status.ActiveRollout != nil {
		t.Fatalf("ActiveRollout should be cleared after success, got %#v", current.Status.ActiveRollout)
	}
}

func TestRolloutReadinessGateToleratesMaxUnavailable(t *testing.T) {
	deployment := readinessDeployment(&nifiv1alpha1.RolloutReadiness{RequireValidComponents: boolPtr(true), MaxUnavailable: 1, TimeoutSeconds: 300})
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	nf.pgs["pg"] = &fakePG{id: "pg", name: "payments", invalidCount: 1}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf}
	snapshot := json.RawMessage(`{"flowContents":{"name":"payments"}}`)

	if _, err := r.reconcileRolloutReadiness(context.Background(), deployment, "https://nifi", "v2", "sha256:new", snapshot); err != nil {
		t.Fatal(err)
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: "payments", Namespace: "default"}, current)
	if !current.Status.Ready {
		t.Fatalf("1 invalid component within maxUnavailable=1 should be ready, syncState=%q", current.Status.SyncState)
	}
}

func TestRolloutReadinessGateTimesOut(t *testing.T) {
	deployment := readinessDeployment(&nifiv1alpha1.RolloutReadiness{RequireValidComponents: boolPtr(true), TimeoutSeconds: 1})
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	deployment.Status.ActiveRollout.ReadinessStartedAt = &past
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	nf.pgs["pg"] = &fakePG{id: "pg", name: "payments", invalidCount: 3}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf}
	snapshot := json.RawMessage(`{"flowContents":{"name":"payments"}}`)

	if _, err := r.reconcileRolloutReadiness(context.Background(), deployment, "https://nifi", "v2", "sha256:new", snapshot); err != nil {
		t.Fatal(err)
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: "payments", Namespace: "default"}, current)
	if current.Status.Ready {
		t.Fatal("deployment must not be Ready after readiness timeout")
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "ReadinessTimeout")
}

func TestRetryRolloutIfAllowed(t *testing.T) {
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 3},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			Rollout: nifiv1alpha1.RolloutStrategy{Strategy: "ApplyOnly", Retry: &nifiv1alpha1.RolloutRetryPolicy{MaxRetries: 2}},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			ProcessGroupID: "pg",
			ActiveRollout:  &nifiv1alpha1.FlowRolloutStatus{Operation: "Rollout", TargetDigest: "sha256:new", StartedAt: metav1.Now()},
		},
	}
	k8sClient, scheme := readinessClient(t, deployment)
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme}

	for attempt := 1; attempt <= 2; attempt++ {
		retried, _, err := r.retryRolloutIfAllowed(context.Background(), deployment, "boom")
		if err != nil {
			t.Fatal(err)
		}
		if !retried {
			t.Fatalf("attempt %d should retry within budget", attempt)
		}
		if deployment.Status.ActiveRollout.RetryCount != int32(attempt) {
			t.Fatalf("retryCount = %d, want %d", deployment.Status.ActiveRollout.RetryCount, attempt)
		}
	}
	// Budget exhausted.
	retried, _, err := r.retryRolloutIfAllowed(context.Background(), deployment, "boom")
	if err != nil {
		t.Fatal(err)
	}
	if retried {
		t.Fatal("retry budget should be exhausted after maxRetries")
	}
}

func TestDrainProcessGroupQueues(t *testing.T) {
	base := func(onTimeout string) *nifiv1alpha1.NiFiFlowDeployment {
		return &nifiv1alpha1.NiFiFlowDeployment{
			Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
				Rollout: nifiv1alpha1.RolloutStrategy{QueuePolicy: &nifiv1alpha1.QueueDrainPolicy{Enabled: true, TimeoutSeconds: 30, OnTimeout: onTimeout}},
			},
		}
	}
	makeReconciler := func(queued int64) (*NiFiFlowDeploymentReconciler, *fakeNiFi) {
		nf := newFakeNiFi()
		nf.connections = []nifi.ConnectionEntity{bgConn("c1", "a", nifi.ConnectableProcessor, "pg", "b", nifi.ConnectableProcessor, "pg")}
		nf.queues["c1"] = queued
		return &NiFiFlowDeploymentReconciler{BlueGreenClient: nf}, nf
	}

	// Empty queue drains immediately.
	r, _ := makeReconciler(0)
	if done, err := r.drainProcessGroupQueues(context.Background(), base("Fail"), "https://nifi", "pg", time.Now()); err != nil || !done {
		t.Fatalf("empty queue: done=%v err=%v", done, err)
	}
	// Non-empty within timeout keeps waiting.
	r, _ = makeReconciler(5)
	if done, err := r.drainProcessGroupQueues(context.Background(), base("Fail"), "https://nifi", "pg", time.Now()); err != nil || done {
		t.Fatalf("draining: done=%v err=%v", done, err)
	}
	// Non-empty past timeout with Fail returns an error.
	r, _ = makeReconciler(5)
	if _, err := r.drainProcessGroupQueues(context.Background(), base("Fail"), "https://nifi", "pg", time.Now().Add(-time.Hour)); err == nil {
		t.Fatal("expected drain failure on timeout with Fail policy")
	}
	// Drop empties the queue and proceeds.
	r, nf := makeReconciler(5)
	if done, err := r.drainProcessGroupQueues(context.Background(), base("Drop"), "https://nifi", "pg", time.Now().Add(-time.Hour)); err != nil || !done {
		t.Fatalf("drop: done=%v err=%v", done, err)
	}
	if nf.queues["c1"] != 0 {
		t.Fatalf("Drop policy should empty the queue, got %d", nf.queues["c1"])
	}
	// Proceed continues despite a non-empty queue.
	r, _ = makeReconciler(5)
	if done, err := r.drainProcessGroupQueues(context.Background(), base("Proceed"), "https://nifi", "pg", time.Now().Add(-time.Hour)); err != nil || !done {
		t.Fatalf("proceed: done=%v err=%v", done, err)
	}
}

func TestReconcileRolloutCancellationInPlace(t *testing.T) {
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 3},
		Spec:       nifiv1alpha1.NiFiFlowDeploymentSpec{Rollout: nifiv1alpha1.RolloutStrategy{Strategy: "ApplyOnly", Cancel: true}},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			ProcessGroupID:       "pg",
			LatestReplaceRequest: &nifiv1alpha1.FlowReplaceRequestStatus{ID: "req-1", Operation: "Rollout"},
			ActiveRollout:        &nifiv1alpha1.FlowRolloutStatus{Operation: "Rollout", Phase: "Replacing", TargetDigest: "sha256:new"},
		},
	}
	k8sClient, scheme := readinessClient(t, deployment)
	nf := newFakeNiFi()
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf}

	if _, err := r.reconcileRolloutCancellation(context.Background(), deployment, "https://nifi"); err != nil {
		t.Fatal(err)
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: "payments", Namespace: "default"}, current)
	if current.Status.ActiveRollout != nil || current.Status.LatestReplaceRequest != nil {
		t.Fatalf("cancellation should clear rollout state, got rollout=%v replace=%v", current.Status.ActiveRollout, current.Status.LatestReplaceRequest)
	}
	if current.Status.SyncState != "RolloutCancelled" {
		t.Fatalf("syncState = %q, want RolloutCancelled", current.Status.SyncState)
	}
}
