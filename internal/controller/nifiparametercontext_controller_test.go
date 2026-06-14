package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeParameterContextClient struct {
	contexts []nifi.ParameterContextEntity
	created  []nifi.ParameterContextEntity
	err      error
}

func (f *fakeParameterContextClient) ListParameterContexts(ctx context.Context, baseURI string) ([]nifi.ParameterContextEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.contexts, nil
}

func (f *fakeParameterContextClient) CreateParameterContext(ctx context.Context, baseURI string, entity nifi.ParameterContextEntity) (*nifi.ParameterContextEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "pc-created"
	created.Component.ID = "pc-created"
	created.Revision.Version = 0
	f.contexts = append(f.contexts, created)
	return &created, nil
}

func TestNiFiParameterContextReconcileCreatesParameterContext(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Description: "Payments parameters",
			Parameters: []nifiv1alpha1.Parameter{
				{Name: "kafka.bootstrap.servers", Value: "kafka:9092"},
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	created := nifiClient.created[0]
	if created.Component.Name != "payments" {
		t.Fatalf("created name = %q, want payments", created.Component.Name)
	}
	if len(created.Component.Parameters) != 1 {
		t.Fatalf("created parameters = %d, want 1", len(created.Component.Parameters))
	}
	value := created.Component.Parameters[0].Parameter.Value
	if value == nil || *value != "kafka:9092" {
		t.Fatalf("created parameter value = %v, want kafka:9092", value)
	}

	current := &nifiv1alpha1.NiFiParameterContext{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("parameter context ready = false, want true")
	}
	if current.Status.NiFiID != "pc-created" {
		t.Fatalf("status nifi id = %q, want pc-created", current.Status.NiFiID)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "ParameterContextReady")
}

func TestNiFiParameterContextReconcileReadsSensitiveParameterSecret(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-db", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("super-secret")},
	}
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Parameters: []nifiv1alpha1.Parameter{
				{
					Name: "db.password",
					SensitiveValueFrom: &nifiv1alpha1.SensitivePropertySource{
						SecretKeyRef: &nifiv1alpha1.SecretKeyRef{
							SecretKeySelector: corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name},
								Key:                  "password",
							},
						},
					},
				},
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, secret, parameterContext)
	nifiClient := &fakeParameterContextClient{}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	parameter := nifiClient.created[0].Component.Parameters[0].Parameter
	if !parameter.Sensitive {
		t.Fatal("sensitive = false, want true")
	}
	if parameter.Value == nil || *parameter.Value != "super-secret" {
		t.Fatalf("sensitive value = %v, want secret value", parameter.Value)
	}
}

func TestNiFiParameterContextReconcileAdoptsExistingByNameWhenAllowed(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: cluster.Name},
			AdoptionPolicy: nifiv1alpha1.AdoptionPolicy{Mode: nifiv1alpha1.AdoptionPolicyAdoptByName},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{
		contexts: []nifi.ParameterContextEntity{
			{ID: "pc-existing", Revision: nifi.Revision{Version: 12}, Component: nifi.ParameterContextComponent{Name: "payments"}},
		},
	}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	if len(nifiClient.created) != 0 {
		t.Fatalf("created count = %d, want 0", len(nifiClient.created))
	}
	current := &nifiv1alpha1.NiFiParameterContext{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.NiFiID != "pc-existing" {
		t.Fatalf("status nifi id = %q, want pc-existing", current.Status.NiFiID)
	}
	if current.Status.Revision.Version != 12 {
		t.Fatalf("status revision = %d, want 12", current.Status.Revision.Version)
	}
}

func TestNiFiParameterContextReconcileDoesNotClaimSpecUpdateWithoutUpdateSupport(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Description: "Changed description",
		},
		Status: nifiv1alpha1.NiFiParameterContextStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				ObservedGeneration: 1,
				Ready:              true,
				NiFiID:             "pc-existing",
				Revision:           nifiv1alpha1.RevisionStatus{Version: 12},
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{
		contexts: []nifi.ParameterContextEntity{
			{ID: "pc-existing", Revision: nifi.Revision{Version: 12}, Component: nifi.ParameterContextComponent{Name: "payments"}},
		},
	}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiParameterContext{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Ready {
		t.Fatal("parameter context ready = true, want false for unsupported update")
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "UpdateNotImplemented")
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	return scheme
}

func readyTestCluster() *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			API: &nifiv1alpha1.NiFiClusterAPISpec{URI: "https://nifi.example.com"},
		},
		Status: nifiv1alpha1.NiFiClusterStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1},
			Endpoint:     "https://nifi.example.com",
		},
	}
}

func newParameterContextTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiParameterContext{}).
		Build()
}

func reconcileParameterContextTwice(t *testing.T, reconciler *NiFiParameterContextReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}
