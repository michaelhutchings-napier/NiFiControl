package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/flowartifact"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeFlowArtifactResolver struct {
	artifact *flowartifact.Artifact
	err      error
}

func (f fakeFlowArtifactResolver) Resolve(ctx context.Context, request flowartifact.Request) (*flowartifact.Artifact, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.artifact, nil
}

func TestNiFiUserReconcileWaitsForCluster(t *testing.T) {
	scheme := testScheme()
	user := &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiUserSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "missing"},
			Identity:   "alice@example.com",
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, user)
	reconciler := &NiFiUserReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: user.Name, Namespace: user.Namespace}}

	reconcileUserTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiUser{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "DependenciesNotReady")
}

func TestNiFiUserGroupReconcileWaitsForMemberUser(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	userGroup := &nifiv1alpha1.NiFiUserGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "operators", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiUserGroupSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Identity:   "operators",
			Users: []nifiv1alpha1.UserGroupMember{
				{UserRef: nifiv1alpha1.LocalObjectReference{Name: "alice"}},
			},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, userGroup)
	reconciler := &NiFiUserGroupReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: userGroup.Name, Namespace: userGroup.Namespace}}

	reconcileUserGroupTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiUserGroup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	if len(current.Status.Dependencies.WaitingFor) != 1 || current.Status.Dependencies.WaitingFor[0] != "NiFiUser/default/alice" {
		t.Fatalf("waitingFor = %#v, want missing alice user", current.Status.Dependencies.WaitingFor)
	}
}

func TestNiFiProcessGroupReconcileWaitsForParameterContext(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	processGroup := &nifiv1alpha1.NiFiProcessGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessGroupSpec{
			ClusterRef:          nifiv1alpha1.ClusterReference{Name: cluster.Name},
			DisplayName:         "Payments",
			ParameterContextRef: &nifiv1alpha1.LocalObjectReference{Name: "payments-prod"},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, processGroup)
	reconciler := &NiFiProcessGroupReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: processGroup.Name, Namespace: processGroup.Namespace}}

	reconcileProcessGroupTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiProcessGroup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	if len(current.Status.Dependencies.WaitingFor) != 1 || current.Status.Dependencies.WaitingFor[0] != "NiFiParameterContext/default/payments-prod" {
		t.Fatalf("waitingFor = %#v, want missing parameter context", current.Status.Dependencies.WaitingFor)
	}
}

func TestNiFiControllerServiceReconcileWaitsForParentProcessGroup(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	controllerService := &nifiv1alpha1.NiFiControllerService{
		ObjectMeta: metav1.ObjectMeta{Name: "dbcp", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiControllerServiceSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: "payments"},
			Type:                  "org.apache.nifi.dbcp.DBCPConnectionPool",
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, controllerService)
	reconciler := &NiFiControllerServiceReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: controllerService.Name, Namespace: controllerService.Namespace}}

	reconcileControllerServiceTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiControllerService{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	if len(current.Status.Dependencies.WaitingFor) != 1 || current.Status.Dependencies.WaitingFor[0] != "NiFiProcessGroup/default/payments" {
		t.Fatalf("waitingFor = %#v, want missing process group", current.Status.Dependencies.WaitingFor)
	}
}

func TestNiFiFlowBundleReconcileWaitsForRegistryClient(t *testing.T) {
	scheme := testScheme()
	flowBundle := &nifiv1alpha1.NiFiFlowBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowBundleSpec{
			Source: nifiv1alpha1.FlowBundleSource{
				Registry: &nifiv1alpha1.RegistryFlowSource{
					RegistryClientRef: nifiv1alpha1.LocalObjectReference{Name: "platform-flows"},
					BucketID:          "bucket-1",
					FlowID:            "flow-1",
				},
			},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, flowBundle)
	reconciler := &NiFiFlowBundleReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowBundle.Name, Namespace: flowBundle.Namespace}}

	reconcileFlowBundleTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiFlowBundle{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	if len(current.Status.Dependencies.WaitingFor) != 1 || current.Status.Dependencies.WaitingFor[0] != "NiFiRegistryClient/default/platform-flows" {
		t.Fatalf("waitingFor = %#v, want missing registry client", current.Status.Dependencies.WaitingFor)
	}
}

func TestNiFiFlowBundleReconcileMarksGitBundleReady(t *testing.T) {
	scheme := testScheme()
	flowBundle := &nifiv1alpha1.NiFiFlowBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowBundleSpec{
			Source: nifiv1alpha1.FlowBundleSource{
				Git: &nifiv1alpha1.GitSource{
					URL: "https://example.test/flows.git",
					Ref: "main",
				},
			},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, flowBundle)
	reconciler := &NiFiFlowBundleReconciler{
		Client: k8sClient, Scheme: scheme,
		ArtifactResolver: fakeFlowArtifactResolver{artifact: &flowartifact.Artifact{Snapshot: *testFlowSnapshot("Payments", "Generate"), Revision: "commit-1"}},
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowBundle.Name, Namespace: flowBundle.Namespace}}

	reconcileFlowBundleTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiFlowBundle{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("flow bundle ready = false, want true")
	}
	if current.Status.ResolvedRevision != "commit-1" {
		t.Fatalf("resolved revision = %q, want commit-1", current.Status.ResolvedRevision)
	}
}

func TestNiFiFlowBundleReconcileResolvesEmbeddedSnapshot(t *testing.T) {
	scheme := testScheme()
	snapshot := testFlowSnapshot("Payments", "Generate")
	_, wantDigest, err := canonicalFlowSnapshot(snapshot, "")
	if err != nil {
		t.Fatal(err)
	}
	flowBundle := &nifiv1alpha1.NiFiFlowBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowBundleSpec{
			Source:  nifiv1alpha1.FlowBundleSource{Snapshot: snapshot},
			Version: "release-1",
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, flowBundle)
	reconciler := &NiFiFlowBundleReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowBundle.Name, Namespace: flowBundle.Namespace}}

	reconcileFlowBundleTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiFlowBundle{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.ArtifactDigest != wantDigest || current.Status.ResolvedRevision != "release-1" {
		t.Fatalf("status ready/digest/revision = %v/%q/%q", current.Status.Ready, current.Status.ArtifactDigest, current.Status.ResolvedRevision)
	}
}

func TestNiFiFlowBundleReconcileRejectsInvalidSnapshot(t *testing.T) {
	scheme := testScheme()
	flowBundle := &nifiv1alpha1.NiFiFlowBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowBundleSpec{
			Source: nifiv1alpha1.FlowBundleSource{Snapshot: &runtime.RawExtension{Raw: []byte(`{"notFlowContents":{}}`)}},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, flowBundle)
	reconciler := &NiFiFlowBundleReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowBundle.Name, Namespace: flowBundle.Namespace}}

	reconcileFlowBundleTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiFlowBundle{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Ready || current.Status.Sync.LastError == "" {
		t.Fatalf("status ready/error = %v/%q", current.Status.Ready, current.Status.Sync.LastError)
	}
	readyReason := ""
	for _, condition := range current.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			readyReason = condition.Reason
		}
	}
	if readyReason != "ArtifactResolutionFailed" {
		t.Fatalf("ready condition reason = %q", readyReason)
	}
}

func TestNiFiFlowDeploymentReconcileWaitsForBundleAndParameterContext(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	flowDeployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef:          nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:              nifiv1alpha1.FlowDeploymentSource{BundleRef: &nifiv1alpha1.LocalObjectReference{Name: "payments"}},
			ParameterContextRef: &nifiv1alpha1.LocalObjectReference{Name: "payments-prod"},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, flowDeployment)
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowDeployment.Name, Namespace: flowDeployment.Namespace}}

	reconcileFlowDeploymentTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	want := []string{"NiFiFlowBundle/default/payments", "NiFiParameterContext/default/payments-prod"}
	if len(current.Status.Dependencies.WaitingFor) != len(want) {
		t.Fatalf("waitingFor = %#v, want %#v", current.Status.Dependencies.WaitingFor, want)
	}
	for i := range want {
		if current.Status.Dependencies.WaitingFor[i] != want[i] {
			t.Fatalf("waitingFor = %#v, want %#v", current.Status.Dependencies.WaitingFor, want)
		}
	}
}

func TestNiFiProcessorReconcileWaitsForParentProcessGroup(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	processor := &nifiv1alpha1.NiFiProcessor{
		ObjectMeta: metav1.ObjectMeta{Name: "generate-payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessorSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: "payments"},
			Type:                  "org.apache.nifi.processors.standard.GenerateFlowFile",
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, processor)
	reconciler := &NiFiProcessorReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: processor.Name, Namespace: processor.Namespace}}

	reconcileProcessorTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiProcessor{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	if len(current.Status.Dependencies.WaitingFor) != 1 || current.Status.Dependencies.WaitingFor[0] != "NiFiProcessGroup/default/payments" {
		t.Fatalf("waitingFor = %#v, want missing process group", current.Status.Dependencies.WaitingFor)
	}
}

func TestNiFiConnectionReconcileWaitsForSourceAndDestination(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	connection := &nifiv1alpha1.NiFiConnection{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-generated", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiConnectionSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source: nifiv1alpha1.ConnectableReference{
				Type: nifiv1alpha1.ConnectableTypeProcessor,
				Name: "generate-payments",
			},
			Destination: nifiv1alpha1.ConnectableReference{
				Type: nifiv1alpha1.ConnectableTypeOutputPort,
				Name: "payments-out",
			},
			SelectedRelationships: []string{"success"},
		},
	}
	k8sClient := newIdentityCanvasTestClient(scheme, cluster, connection)
	reconciler := &NiFiConnectionReconciler{Client: k8sClient, Scheme: scheme}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: connection.Name, Namespace: connection.Namespace}}

	reconcileConnectionTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiConnection{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Dependencies.Ready {
		t.Fatal("dependencies ready = true, want false")
	}
	want := []string{"NiFiProcessor/default/generate-payments", "NiFiOutputPort/default/payments-out"}
	if len(current.Status.Dependencies.WaitingFor) != len(want) {
		t.Fatalf("waitingFor = %#v, want %#v", current.Status.Dependencies.WaitingFor, want)
	}
	for i := range want {
		if current.Status.Dependencies.WaitingFor[i] != want[i] {
			t.Fatalf("waitingFor = %#v, want %#v", current.Status.Dependencies.WaitingFor, want)
		}
	}
}

func newIdentityCanvasTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(
			&nifiv1alpha1.NiFiCluster{},
			&nifiv1alpha1.NiFiRegistryClient{},
			&nifiv1alpha1.NiFiParameterContext{},
			&nifiv1alpha1.NiFiUser{},
			&nifiv1alpha1.NiFiUserGroup{},
			&nifiv1alpha1.NiFiProcessGroup{},
			&nifiv1alpha1.NiFiControllerService{},
			&nifiv1alpha1.NiFiFlowBundle{},
			&nifiv1alpha1.NiFiFlowDeployment{},
			&nifiv1alpha1.NiFiProcessor{},
			&nifiv1alpha1.NiFiInputPort{},
			&nifiv1alpha1.NiFiOutputPort{},
			&nifiv1alpha1.NiFiConnection{},
			&nifiv1alpha1.NiFiReportingTask{},
			&nifiv1alpha1.NiFiFunnel{},
			&nifiv1alpha1.NiFiLabel{},
		).
		Build()
}

func reconcileUserTwice(t *testing.T, reconciler *NiFiUserReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileUserGroupTwice(t *testing.T, reconciler *NiFiUserGroupReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileProcessGroupTwice(t *testing.T, reconciler *NiFiProcessGroupReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileControllerServiceTwice(t *testing.T, reconciler *NiFiControllerServiceReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileFlowBundleTwice(t *testing.T, reconciler *NiFiFlowBundleReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileFlowDeploymentTwice(t *testing.T, reconciler *NiFiFlowDeploymentReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileProcessorTwice(t *testing.T, reconciler *NiFiProcessorReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileConnectionTwice(t *testing.T, reconciler *NiFiConnectionReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}
